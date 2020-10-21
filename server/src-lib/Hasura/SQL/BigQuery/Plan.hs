{-# OPTIONS_GHC -fno-warn-type-defaults #-}
-- | Planning BigQuery queries and subscriptions.

module Hasura.SQL.BigQuery.Plan
  ( planNoPlan
  , planNoPlanMap
  , planMultiplex
  , test
  , runSelect
  ) where

import           Control.Applicative
import           Control.Monad.Trans
import           Control.Monad.Trans.State.Strict
import           Control.Monad.Validate
import           Data.Aeson (encode, object, (.=))
import           Data.Bifunctor
import qualified Data.ByteString.Char8 as S8
import qualified Data.ByteString.Lazy as L
import           Data.Functor
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashMap.Strict.InsOrd as OMap
import           Data.Int
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import qualified Data.Text as T
import qualified Hasura.GraphQL.Context as Graphql
import qualified Hasura.GraphQL.Execute.Query as Query
import qualified Hasura.GraphQL.Parser.Column as Graphql
import qualified Hasura.GraphQL.Parser.Schema as PS
import qualified Hasura.SQL.BigQuery.FromIr as BigQuery
import qualified Hasura.SQL.BigQuery.FromIr as FromIr
import           Hasura.SQL.BigQuery.ToQuery
import           Hasura.SQL.BigQuery.Types as BigQuery
import qualified Hasura.SQL.DML as Sql
import qualified Hasura.SQL.Types as Sql
import qualified Hasura.SQL.Value as Sql
import qualified Language.GraphQL.Draft.Syntax as G
import           Network.HTTP.Conduit
import           Prelude
import           System.Environment

--------------------------------------------------------------------------------
-- Testing

test :: OMap.InsOrdHashMap G.Name (Graphql.SubscriptionRootField Graphql.UnpreparedValue)
     -> IO ()
test i =
  case rootFields of
    Left _ -> putStrLn "errored out"
    Right fields ->
      putStrLn
        (unlines
           [ "BigQuery.test"
           , "\naliases"
           , show (void i)
           , "\nroots"
           , show i
           , "\nprepared"
           , show rootFields
           , "\ncollapsed"
           , show (collapseMap fields)
           , "\nmultiplexed"
           , show (multiplexRootReselect (collapseMap fields))
           , "\nprinted (pretty)"
           , T.unpack
               (toTextPretty
                  (fromSelect (multiplexRootReselect (collapseMap fields))))
           , "\nprinted (flat)"
           , T.unpack
               (toTextFlat
                  (fromSelect (multiplexRootReselect (collapseMap fields))))
           ])
  where
    rootFields =
      case traverse
             (flip
                evalStateT
                PrepareState {positionalArguments = 0, namedArguments = mempty} .
              Query.traverseQueryRootField prepareValueMultiplex)
             i of
        Left e -> error (show e)
        Right v -> do
          x <- runValidate $ BigQuery.runFromIr $ traverse BigQuery.fromRootField v
          pure x

--------------------------------------------------------------------------------
-- Run a select query against BigQuery

runSelect :: Select -> IO ()
runSelect select0 = do
  bigqueryaccesstoken <- getEnvUnline "BIGQUERYACCESSTOKEN"
  bigqueryapitoken <- getEnvUnline "BIGQUERYAPITOKEN"
  bigqueryprojectname <- getEnvUnline "BIGQUERYPROJECTNAME"
  req <-
    parseUrl
      ("https://content-bigquery.googleapis.com/bigquery/v2/projects/" <>
       bigqueryprojectname <>
       "/queries?alt=json&key=" <>
       bigqueryapitoken)
  mgr <- newManager tlsManagerSettings
  let body' = "{\"query\":\"select * from chinook.Artist\"}"
      body =
        encode
          (object
             [ "query" .= toTextFlat (fromSelect select0)
             , "useLegacySql" .= False -- Important, it makes `quotes` work properly.
             ])
  {-L.putStrLn ("Request body:\n" <> body)-}
  let request =
        req
          { requestHeaders =
              [ ("Authorization", "Bearer " <> S8.pack bigqueryaccesstoken)
              , ("Content-Type", "application/json")
              , ("User-Agent", "curl/7.54")
              ]
          , checkResponse = \_ resp -> pure ()
          , method = "POST"
          , requestBody = RequestBodyLBS body
          }
  {-print request-}
  pure ()
  -- httpLbs request mgr >>= L.putStr . responseBody
  where
    getEnvUnline key = do
      value <- fmap (concat . take 1 . lines) (getEnv key)
      print (key, value)
      pure value

--------------------------------------------------------------------------------
-- Top-level planner

-- | Plan a query without prepare/exec.
planNoPlan ::
     Graphql.RootField (Graphql.QueryDB Graphql.UnpreparedValue) void1 void2 void3
  -> Either PrepareError Select
planNoPlan unpreparedRoot = do
  rootField <- Query.traverseQueryRootField prepareValueNoPlan unpreparedRoot
  select <-
    first
      FromIrError
      (runValidate (BigQuery.runFromIr (BigQuery.fromRootField rootField)))
  pure
    select
      { selectAsStruct =
          case selectAsStruct select of
            NoStruct -> NoStruct
            AsStruct -> AsStruct
      }

planMultiplex ::
     OMap.InsOrdHashMap G.Name (Graphql.SubscriptionRootField Graphql.UnpreparedValue)
  -> Either PrepareError Select
planMultiplex unpreparedMap = do
  rootFieldMap <-
    evalStateT
      (traverse
         (Query.traverseQueryRootField prepareValueMultiplex)
         unpreparedMap)
      emptyPrepareState
  selectMap <-
    first
      FromIrError
      (runValidate (BigQuery.runFromIr (traverse BigQuery.fromRootField rootFieldMap)))
  pure (multiplexRootReselect (collapseMap selectMap))

-- | Plan a query without prepare/exec.
planNoPlanMap ::
     OMap.InsOrdHashMap G.Name (Graphql.SubscriptionRootField Graphql.UnpreparedValue)
  -> Either PrepareError Reselect
planNoPlanMap unpreparedMap = do
  rootFieldMap <-
    traverse (Query.traverseQueryRootField prepareValueNoPlan) unpreparedMap
  selectMap <-
    first
      FromIrError
      (runValidate (BigQuery.runFromIr (traverse BigQuery.fromRootField rootFieldMap)))
  pure (collapseMap selectMap)

--------------------------------------------------------------------------------
-- Converting a root field into a BigQuery select statement

-- | Collapse a set of selects into a single select that projects
-- these as subselects.
collapseMap :: OMap.InsOrdHashMap G.Name Select
            -> Reselect
collapseMap selects =
  Reselect
    { reselectAsStruct = AsStruct
    , reselectWhere = Where mempty
    , reselectProjections =
        NE.fromList (map projectSelect (OMap.toList selects))
    }
  where
    projectSelect :: (G.Name, Select) -> Projection
    projectSelect (name, select) =
      ExpressionProjection
        (Aliased
           { aliasedThing = SelectExpression select
           , aliasedAlias = G.unName name
           })

--------------------------------------------------------------------------------
-- Session variables

globalSessionExpression :: BigQuery.Expression
globalSessionExpression =
  ValueExpression (TextValue "TODO: sessionExpression")

-- TODO: real env object.
envObjectExpression :: BigQuery.Expression
envObjectExpression =
  ValueExpression (TextValue "[{\"result_id\":1,\"result_vars\":{\"synthetic\":[10]}}]")

--------------------------------------------------------------------------------
-- Resolving values

data PrepareError
  = UVLiteralNotSupported
  | SessionVarNotSupported
  | UnsupportedPgType Sql.PGScalarValue
  | FromIrError (NonEmpty FromIr.Error)
  deriving (Show, Eq)

data PrepareState = PrepareState
  { positionalArguments :: !Integer
  , namedArguments :: !(HashMap G.Name Graphql.PGColumnValue)
  }

emptyPrepareState :: PrepareState
emptyPrepareState =
  PrepareState {positionalArguments = 0, namedArguments = mempty}

-- | Prepare a value without any query planning; we just execute the
-- query with the values embedded.
prepareValueNoPlan :: Graphql.UnpreparedValue -> Either PrepareError BigQuery.Expression
prepareValueNoPlan =
  \case
    Graphql.UVLiteral (_ :: Sql.SQLExp) -> Left UVLiteralNotSupported
    Graphql.UVSession -> pure (JsonQueryExpression globalSessionExpression)
    Graphql.UVSessionVar _typ _text -> Left SessionVarNotSupported
    Graphql.UVParameter Graphql.PGColumnValue {pcvValue = Sql.WithScalarType {pstValue}} _mVariableInfo ->
      case fromPgScalarValue pstValue of
        Nothing -> Left (UnsupportedPgType pstValue)
        Just value -> pure (ValueExpression value)

-- | Prepare a value for multiplexed queries.
prepareValueMultiplex ::
     Graphql.UnpreparedValue
  -> StateT PrepareState (Either PrepareError) BigQuery.Expression
prepareValueMultiplex =
  \case
    Graphql.UVLiteral (_ :: Sql.SQLExp) -> lift (Left UVLiteralNotSupported)
    Graphql.UVSession ->
      pure (JsonQueryExpression globalSessionExpression)
    Graphql.UVSessionVar _typ _text -> lift (Left SessionVarNotSupported)
    Graphql.UVParameter pgcolumnvalue mVariableInfo ->
      case fmap PS.getName mVariableInfo of
        Nothing -> do
          index <- gets positionalArguments
          modify' (\s -> s {positionalArguments = index + 1})
          pure
            (JsonValueExpression
               (ColumnExpression
                  FieldName
                    { fieldNameEntity = rowAlias
                    , fieldName = resultVarsAlias
                    })
               (RootPath `FieldPath` "synthetic" `IndexPath` index))
        Just name -> do
          modify
            (\s ->
               s
                 { namedArguments =
                     HM.insert name pgcolumnvalue (namedArguments s)
                 })
          pure
            (JsonValueExpression
               envObjectExpression
               (RootPath `FieldPath` "query" `FieldPath` G.unName name))

--------------------------------------------------------------------------------
-- Producing the correct SQL-level list comprehension to multiplex a query

-- Problem description:
--
-- Generate a query that repeats the same query N times but with
-- certain slots replaced:
--
-- [ Select x y | (x,y) <- [..] ]
--

multiplexRootReselect :: BigQuery.Reselect -> BigQuery.Select
multiplexRootReselect rootReselect =
  Select
    { selectTop = NoTop
    , selectWiths = []
    , selectProjections =
        NE.fromList
          [ FieldNameProjection
              Aliased
                { aliasedThing =
                    FieldName
                      {fieldNameEntity = rowAlias, fieldName = resultIdAlias}
                , aliasedAlias = resultIdAlias
                }
          , ExpressionProjection
              Aliased
                { aliasedThing =
                    JsonQueryExpression
                      (ColumnExpression
                         (FieldName
                            { fieldNameEntity = resultAlias
                            , fieldName = BigQuery.jsonFieldName
                            }))
                , aliasedAlias = resultAlias
                }
          ]
    , selectFrom =
        FromOpenJson
          Aliased
            { aliasedThing =
                OpenJson
                  { openJsonExpression = envObjectExpression
                  , openJsonWith =
                      NE.fromList
                        [IntField resultIdAlias, JsonField resultVarsAlias]
                  }
            , aliasedAlias = rowAlias
            }
    , selectJoins =
        [ LeftOuterJoin
            { joinSource = JoinReselect rootReselect
            , joinJoinAlias =
                JoinAlias
                  { joinAliasEntity = resultAlias
                  -- , joinAliasField = Just BigQuery.jsonFieldName
                  }
                  , joinOn = BigQuery.trueExpression
            , joinProjections = NE.fromList [StarProjection]
            }
        ]
    , selectWhere = Where mempty
    , selectAsStruct = AsStruct
    , selectOrderBy = Nothing
    , selectOffset = Nothing
    , selectAsJson = AsJsonArray
    }

resultIdAlias :: T.Text
resultIdAlias = "result_id"

resultVarsAlias :: T.Text
resultVarsAlias = "result_vars"

resultAlias :: T.Text
resultAlias = "result"

rowAlias :: T.Text
rowAlias = "row"

--------------------------------------------------------------------------------
-- PG compat

-- | Convert from PG values to Value. Later, this shouldn't be
-- necessary; the value should come in as a value already.
fromPgScalarValue :: Sql.PGScalarValue -> Maybe Value
fromPgScalarValue =
  \case
    Sql.PGValInteger i32 ->
      pure (IntValue ((fromIntegral :: Int32 -> Int) i32))
    Sql.PGValSmallInt i16 ->
      pure (IntValue ((fromIntegral :: Int16 -> Int) i16))
    Sql.PGValBigInt i64 ->
      pure (IntValue ((fromIntegral :: Int64 -> Int) i64))
    Sql.PGValFloat float -> pure (FloatValue float)
    Sql.PGValDouble double -> pure (DoubleValue double)
    Sql.PGNull _pgscalartype -> pure NullValue
    Sql.PGValBoolean bool -> pure (BoolValue bool)
    Sql.PGValVarchar text -> pure (TextValue text)
    Sql.PGValText text -> pure (TextValue text)
    Sql.PGValDate _day -> Nothing
    Sql.PGValTimeStamp _localtime -> Nothing
     -- For these, see Datetime2 in Database.ODBC.SQLServer.
    Sql.PGValTimeStampTZ _utctime -> Nothing -- TODO: Sql.PGValTimeStampTZ utctime
    Sql.PGValTimeTZ _zonedtimeofday -> Nothing -- TODO: Sql.PGValTimeTZ zonedtimeofday
    Sql.PGValNumeric _scientific -> Nothing -- TODO: Sql.PGValNumeric scientific
    Sql.PGValMoney _scientific -> Nothing -- TODO: Sql.PGValMoney scientific
    Sql.PGValChar _char -> Nothing -- TODO: Sql.PGValChar char
    Sql.PGValCitext _text -> Nothing -- TODO: Sql.PGValCitext text
     -- I'm not sure whether it's fine to encode as string, because
     -- that's what SQL Server treats JSON as. But wrapping it in a
     -- JsonQueryExpression might help.
    Sql.PGValJSON _json -> Nothing -- TODO: Sql.PGValJSON json
    Sql.PGValJSONB _jsonb -> Nothing -- TODO: Sql.PGValJSONB jsonb
    Sql.PGValGeo _geometrywithcrs -> Nothing -- TODO: Sql.PGValGeo geometrywithcrs
    Sql.PGValRaster _rasterwkb -> Nothing -- TODO: Sql.PGValRaster rasterwkb
     -- There is a UUID type in SQL Server, but it needs research.
    Sql.PGValUUID _uuid -> Nothing -- TODO: Sql.PGValUUID uuid
    Sql.PGValUnknown _text -> Nothing -- TODO: Sql.PGValUnknown text
