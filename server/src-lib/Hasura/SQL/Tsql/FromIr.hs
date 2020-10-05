-- | Translate from the DML to the TSql dialect.

module Hasura.SQL.Tsql.FromIr
  ( fromSelectRows
  , mkSQLSelect
  , fromSelectAggregate
  , Error(..)
  , runFromIr
  , FromIr
  ) where

import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.Trans.State.Strict
import           Control.Monad.Validate
import           Control.Monad.Writer.Strict
import           Data.Foldable
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe
import           Data.Proxy
import           Data.Sequence (Seq)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Read as T
import qualified Database.ODBC.SQLServer as Odbc
import qualified Hasura.GraphQL.Parser.Column as Graphql
import qualified Hasura.RQL.DML.Select.Types as Ir
import qualified Hasura.RQL.Types.BoolExp as Ir
import qualified Hasura.RQL.Types.Column as Ir
import qualified Hasura.RQL.Types.Common as Ir
import qualified Hasura.RQL.Types.DML as Ir
import qualified Hasura.SQL.DML as Sql
import           Hasura.SQL.Tsql.Types as Tsql
import qualified Hasura.SQL.Types as Sql
import           Prelude

--------------------------------------------------------------------------------
-- Types

-- | Most of these errors should be checked for legitimacy.
data Error
  = FromTypeUnsupported (Ir.SelectFromG Graphql.UnpreparedValue)
  | NoOrderSpecifiedInOrderBy
  | MalformedAgg
  | FieldTypeUnsupportedForNow (Ir.AnnFieldG Graphql.UnpreparedValue)
  | AggTypeUnsupportedForNow (Ir.TableAggregateFieldG Graphql.UnpreparedValue)
  | NoProjectionFields
  | NoAggregatesMustBeABug
  | UnsupportedArraySelect (Ir.ArraySelectG Graphql.UnpreparedValue)
  | UnsupportedOpExpG (Ir.OpExpG Graphql.UnpreparedValue)
  | UnsupportedSQLExp Graphql.UnpreparedValue
  | UnsupportedDistinctOn
  | InvalidIntegerishSql Sql.SQLExp
  | DistinctIsn'tSupported
  deriving (Show, Eq)

-- | The base monad used throughout this module for all conversion
-- functions.
--
-- It's a Validate, so it'll continue going when it encounters errors
-- to accumulate as many as possible.
--
-- It also contains a mapping from entity prefixes to counters. So if
-- my prefix is "table" then there'll be a counter that lets me
-- generate table1, table2, etc. Same for any other prefix needed
-- (e.g. names for joins).
--
-- A ReaderT is used around this in most of the module too, for
-- setting the current entity that a given field name refers to. See
-- @fromPGCol@.
newtype FromIr a = FromIr
  { unFromIr :: StateT (Map Text Int) (Validate (NonEmpty Error)) a
  } deriving (Functor, Applicative, Monad, MonadValidate (NonEmpty Error))

data StringifyNumbers
  = StringifyNumbers
  | LeaveNumbersAlone
  deriving (Eq)

--------------------------------------------------------------------------------
-- Runners

runFromIr :: FromIr a -> Validate (NonEmpty Error) a
runFromIr fromIr = evalStateT (unFromIr fromIr) mempty

--------------------------------------------------------------------------------
-- Similar rendition of old API

mkSQLSelect ::
     Ir.JsonAggSelect
  -> Ir.AnnSelectG (Ir.AnnFieldsG Graphql.UnpreparedValue) Graphql.UnpreparedValue
  -> FromIr Tsql.Select
mkSQLSelect jsonAggSelect annSimpleSel =
  case jsonAggSelect of
    Ir.JASMultipleRows -> fromSelectRows annSimpleSel
    Ir.JASSingleObject -> do
      select <- fromSelectRows annSimpleSel
      pure select {selectFor = JsonFor JsonSingleton, selectTop = Top 1}

--------------------------------------------------------------------------------
-- Top-level exported functions

fromSelectRows :: Ir.AnnSelectG (Ir.AnnFieldsG Graphql.UnpreparedValue) Graphql.UnpreparedValue -> FromIr Tsql.Select
fromSelectRows annSelectG = do
  selectFrom <-
    case from of
      Ir.FromTable qualifiedObject -> fromQualifiedTable qualifiedObject
      _ -> refute (pure (FromTypeUnsupported from))
  Args { argsOrderBy
       , argsWhere
       , argsJoins
       , argsTop
       , argsDistinct = Proxy
       , argsOffset
       , argsExistingJoins
       } <- runReaderT (fromSelectArgsG args) (fromAlias selectFrom)
  fieldSources <-
    runReaderT
      (traverse (fromAnnFieldsG argsExistingJoins stringifyNumbers) fields)
      (fromAlias selectFrom)
  filterExpression <-
    runReaderT (fromAnnBoolExp permFilter) (fromAlias selectFrom)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> refute (pure NoProjectionFields)
      Just ne -> pure ne
  pure
    Select
      { selectOrderBy = argsOrderBy
      , selectTop = permissionBasedTop <> argsTop
      , selectProjections
      , selectFrom
      , selectJoins = argsJoins <> mapMaybe fieldSourceJoin fieldSources
      , selectWhere = argsWhere <> Where [filterExpression]
      , selectFor = JsonFor JsonArray
      , selectOffset = argsOffset
      }
  where
    Ir.AnnSelectG { _asnFields = fields
                  , _asnFrom = from
                  , _asnPerm = perm
                  , _asnArgs = args
                  , _asnStrfyNum = num
                  } = annSelectG
    Ir.TablePerm {_tpLimit = mPermLimit, _tpFilter = permFilter} = perm
    permissionBasedTop =
      case mPermLimit of
        Nothing -> NoTop
        Just limit -> Top limit
    stringifyNumbers =
      if num
        then StringifyNumbers
        else LeaveNumbersAlone

fromSelectAggregate ::
     Ir.AnnSelectG [(Ir.FieldName, Ir.TableAggregateFieldG Graphql.UnpreparedValue)] Graphql.UnpreparedValue
  -> FromIr Tsql.Select
fromSelectAggregate annSelectG = do
  selectFrom <-
    case from of
      Ir.FromTable qualifiedObject -> fromQualifiedTable qualifiedObject
      _ -> refute (pure (FromTypeUnsupported from))
  fieldSources <-
    runReaderT (traverse fromTableAggregateFieldG fields) (fromAlias selectFrom)
  filterExpression <-
    runReaderT (fromAnnBoolExp permFilter) (fromAlias selectFrom)
  Args { argsOrderBy
       , argsWhere
       , argsJoins
       , argsTop
       , argsDistinct = Proxy
       , argsOffset
       } <- runReaderT (fromSelectArgsG args) (fromAlias selectFrom)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> refute (pure NoProjectionFields)
      Just ne -> pure ne
  pure
    Select
      { selectProjections
      , selectTop = permissionBasedTop <> argsTop
      , selectFrom
      , selectJoins = argsJoins <> mapMaybe fieldSourceJoin fieldSources
      , selectWhere = argsWhere <> Where [filterExpression]
      , selectFor = JsonFor JsonSingleton
      , selectOrderBy = argsOrderBy
      , selectOffset = argsOffset
      }
  where
    Ir.AnnSelectG { _asnFields = fields
                  , _asnFrom = from
                  , _asnPerm = perm
                  , _asnArgs = args
                  , _asnStrfyNum = _num -- TODO: Do we ignore this for aggregates?
                  } = annSelectG
    Ir.TablePerm {_tpLimit = mPermLimit, _tpFilter = permFilter} = perm
    permissionBasedTop =
      case mPermLimit of
        Nothing -> NoTop
        Just limit -> Top limit

--------------------------------------------------------------------------------
-- GraphQL Args

data Args = Args
  { argsWhere :: Where
  , argsOrderBy :: Maybe (NonEmpty OrderBy)
  , argsJoins :: [Join]
  , argsTop :: Top
  , argsOffset :: Maybe Expression
  , argsDistinct :: Proxy (Maybe (NonEmpty FieldName))
  , argsExistingJoins :: Map Sql.QualifiedTable EntityAlias
  } deriving (Show)

data UnfurledJoin = UnfurledJoin
  { unfurledJoin :: Join
  , unfurledObjectTableAlias :: Maybe (Sql.QualifiedTable, EntityAlias)
    -- ^ Recorded if we joined onto an object relation.
  } deriving (Show)

fromSelectArgsG :: Ir.SelectArgsG Graphql.UnpreparedValue -> ReaderT EntityAlias FromIr Args
fromSelectArgsG selectArgsG = do
  argsWhere <-
    maybe (pure mempty) (fmap (Where . pure) . fromAnnBoolExp) mannBoolExp
  argsTop <- maybe (pure mempty) (pure . Top) mlimit
  argsOffset <-
    maybe (pure Nothing) (fmap Just . lift . fromSQLExpAsInt) moffset
  argsDistinct' <-
    maybe (pure Nothing) (fmap Just . traverse fromPGCol) mdistinct
  -- Not supported presently, per Vamshi:
  --
  -- > It is hardly used and we don't have to go to great lengths to support it.
  --
  -- But placeholdering the code so that when it's ready to be used,
  -- you can just drop the Proxy wrapper.
  argsDistinct <-
    case argsDistinct' of
      Nothing -> pure Proxy
      Just {} -> refute (pure DistinctIsn'tSupported)
  (argsOrderBy, joins) <-
    runWriterT (traverse fromAnnOrderByItemG (maybe [] toList orders))
  -- Any object-relation joins that we generated, we record their
  -- generated names into a mapping.
  let argsExistingJoins =
        M.fromList (mapMaybe unfurledObjectTableAlias (toList joins))
  pure
    Args
      { argsJoins = toList (fmap unfurledJoin joins)
      , argsOrderBy = NE.nonEmpty argsOrderBy
      , ..
      }
  where
    Ir.SelectArgs { _saWhere = mannBoolExp
                  , _saLimit = mlimit
                  , _saOffset = moffset
                  , _saDistinct = mdistinct
                  , _saOrderBy = orders
                  } = selectArgsG

-- | Produce a valid ORDER BY construct, telling about any joins
-- needed on the side.
fromAnnOrderByItemG ::
     Ir.AnnOrderByItemG Graphql.UnpreparedValue -> WriterT (Seq UnfurledJoin) (ReaderT EntityAlias FromIr) OrderBy
fromAnnOrderByItemG Ir.OrderByItemG {obiType, obiColumn, obiNulls} = do
  orderByFieldName <- unfurlAnnOrderByElement obiColumn
  let morderByOrder =
        fmap
          (\case
             Sql.OTAsc -> AscOrder
             Sql.OTDesc -> DescOrder)
          (fmap Ir.unOrderType obiType)
  let orderByNullsOrder =
        case fmap Ir.unNullsOrder obiNulls of
          Nothing -> NullsAnyOrder
          Just nullsOrder ->
            case nullsOrder of
              Sql.NFirst -> NullsFirst
              Sql.NLast -> NullsLast
  case morderByOrder of
    Just orderByOrder -> pure OrderBy {..}
    Nothing -> refute (pure NoOrderSpecifiedInOrderBy)

-- | Unfurl the nested set of object relations (tell'd in the writer)
-- that are terminated by field name (Ir.AOCColumn and
-- Ir.AOCArrayAggregation).
unfurlAnnOrderByElement ::
     Ir.AnnOrderByElement Graphql.UnpreparedValue -> WriterT (Seq UnfurledJoin) (ReaderT EntityAlias FromIr) FieldName
unfurlAnnOrderByElement =
  \case
    Ir.AOCColumn pgColumnInfo -> do
      fieldName <- lift (fromPGColumnInfo pgColumnInfo)
      pure fieldName
    Ir.AOCObjectRelation Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp annOrderByElementG -> do
      selectFrom <- lift (lift (fromQualifiedTable table))
      joinAliasEntity <-
        lift (lift (generateEntityAlias (ForOrderAlias (tableNameText table))))
      foreignKeyConditions <- lift (fromMapping selectFrom mapping)
      -- TODO: Because these object relations are re-used by regular
      -- object mapping queries, this WHERE may be unnecessarily
      -- restrictive. But I actually don't know from where such an
      -- expression arises in the source GraphQL syntax.
      --
      -- Worst case scenario, we could put the WHERE in the key of the
      -- Map in 'argsExistingJoins'. That would guarantee only equal
      -- selects are re-used.
      whereExpression <-
        lift (local (const (fromAlias selectFrom)) (fromAnnBoolExp annBoolExp))
      tell
        (pure
           UnfurledJoin
             { unfurledJoin =
                 Join
                   { joinSource =
                       JoinSelect
                         Select
                           { selectTop = NoTop
                           , selectProjections = NE.fromList [StarProjection]
                           , selectFrom
                           , selectJoins = []
                           , selectWhere =
                               Where (foreignKeyConditions <> [whereExpression])
                           , selectFor = NoFor
                           , selectOrderBy = Nothing
                           , selectOffset = Nothing
                           }
                   , joinJoinAlias =
                       JoinAlias {joinAliasEntity, joinAliasField = Nothing}
                   }
             , unfurledObjectTableAlias = Just (table, EntityAlias joinAliasEntity)
             })
      local
        (const (EntityAlias joinAliasEntity))
        (unfurlAnnOrderByElement annOrderByElementG)
    Ir.AOCArrayAggregation Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp annAggregateOrderBy -> do
      selectFrom <- lift (lift (fromQualifiedTable table))
      let alias = aggFieldName
      joinAliasEntity <-
        lift (lift (generateEntityAlias (ForOrderAlias (tableNameText table))))
      foreignKeyConditions <- lift (fromMapping selectFrom mapping)
      whereExpression <-
        lift (local (const (fromAlias selectFrom)) (fromAnnBoolExp annBoolExp))
      aggregate <-
        lift
          (local
             (const (fromAlias selectFrom))
             (case annAggregateOrderBy of
                Ir.AAOCount -> pure (CountAggregate StarCountable)
                Ir.AAOOp text pgColumnInfo -> do
                  fieldName <- fromPGColumnInfo pgColumnInfo
                  pure (OpAggregate text (pure (ColumnExpression fieldName)))))
      tell
        (pure
           (UnfurledJoin
              { unfurledJoin =
                  Join
                    { joinSource =
                        JoinSelect
                          Select
                            { selectTop = NoTop
                            , selectProjections =
                                NE.fromList
                                  [ AggregateProjection
                                      Aliased
                                        { aliasedThing = aggregate
                                        , aliasedAlias = alias
                                        }
                                  ]
                            , selectFrom
                            , selectJoins = []
                            , selectWhere =
                                Where
                                  (foreignKeyConditions <> [whereExpression])
                            , selectFor = NoFor
                            , selectOrderBy = Nothing
                            , selectOffset = Nothing
                            }
                    , joinJoinAlias =
                        JoinAlias {joinAliasEntity, joinAliasField = Nothing}
                    }
              , unfurledObjectTableAlias = Nothing
              }))
      pure FieldName {fieldNameEntity = joinAliasEntity, fieldName = alias}

--------------------------------------------------------------------------------
-- Conversion functions

tableNameText :: Sql.QualifiedObject Sql.TableName -> Text
tableNameText qualifiedObject = qname
  where
    Sql.QualifiedObject {qName = Sql.TableName qname} = qualifiedObject

-- | This is really the start where you query the base table,
-- everything else is joins attached to it.
fromQualifiedTable :: Sql.QualifiedObject Sql.TableName -> FromIr From
fromQualifiedTable qualifiedObject = do
  alias <- generateEntityAlias (TableTemplate qname)
  pure
    (FromQualifiedTable
       (Aliased
          { aliasedThing =
              TableName {tableName = qname, tableNameSchema = schemaName}
          , aliasedAlias = alias
          }))
  where
    Sql.QualifiedObject { qSchema = Sql.SchemaName schemaName
                         -- TODO: Consider many x.y.z. in schema name.
                        , qName = Sql.TableName qname
                        } = qualifiedObject

fromAnnBoolExp ::
     Ir.GBoolExp (Ir.AnnBoolExpFld Graphql.UnpreparedValue)
  -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExp = traverse fromAnnBoolExpFld >=> fromGBoolExp

fromAnnBoolExpFld ::
     Ir.AnnBoolExpFld Graphql.UnpreparedValue -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExpFld =
  \case
    Ir.AVCol pgColumnInfo opExpGs -> do
      expression <- fmap ColumnExpression (fromPGColumnInfo pgColumnInfo)
      expressions <- traverse (lift . fromOpExpG expression) opExpGs
      pure (AndExpression expressions)
    Ir.AVRel Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp -> do
      selectFrom <- lift (fromQualifiedTable table)
      foreignKeyConditions <- fromMapping selectFrom mapping
      whereExpression <-
        local (const (fromAlias selectFrom)) (fromAnnBoolExp annBoolExp)
      pure
        (ExistsExpression
           Select
             { selectOrderBy = Nothing
             , selectProjections =
                 NE.fromList
                   [ ExpressionProjection
                       (Aliased
                          { aliasedThing = trueExpression
                          , aliasedAlias = existsFieldName
                          })
                   ]
             , selectFrom
             , selectJoins = mempty
             , selectWhere = Where (foreignKeyConditions <> [whereExpression])
             , selectTop = NoTop
             , selectFor = NoFor
             , selectOffset = Nothing
             })

fromPGColumnInfo :: Ir.PGColumnInfo -> ReaderT EntityAlias FromIr FieldName
fromPGColumnInfo Ir.PGColumnInfo {pgiColumn = pgCol} = do
  EntityAlias {entityAliasText} <- ask
  pure
    (FieldName
       {fieldName = Sql.getPGColTxt pgCol, fieldNameEntity = entityAliasText})

fromGExists :: Ir.GExists Expression -> ReaderT EntityAlias FromIr Select
fromGExists Ir.GExists {_geTable, _geWhere} = do
  selectFrom <- lift (fromQualifiedTable _geTable)
  whereExpression <-
    local (const (fromAlias selectFrom)) (fromGBoolExp _geWhere)
  pure
    Select
      { selectOrderBy = Nothing
      , selectProjections =
          NE.fromList
            [ ExpressionProjection
                (Aliased
                   { aliasedThing = trueExpression
                   , aliasedAlias = existsFieldName
                   })
            ]
      , selectFrom
      , selectJoins = mempty
      , selectWhere = Where [whereExpression]
      , selectTop = NoTop
      , selectFor = NoFor
      , selectOffset = Nothing
      }

--------------------------------------------------------------------------------
-- Sources of projected fields
--
-- Because in the IR, a field projected can be a foreign object, we
-- have to both generate a projection AND on the side generate a join.
--
-- So a @FieldSource@ couples the idea of the projected thing and the
-- source of it.

data FieldSource
  = ExpressionFieldSource (Aliased Expression)
  | JoinFieldSource (Aliased Join)
  | AggregateFieldSource (NonEmpty (Aliased Aggregate))
  deriving (Eq, Show)

fromTableAggregateFieldG ::
     (Ir.FieldName, Ir.TableAggregateFieldG Graphql.UnpreparedValue) -> ReaderT EntityAlias FromIr FieldSource
fromTableAggregateFieldG (Ir.FieldName name, field) =
  case field of
    Ir.TAFAgg (aggregateFields :: [(Ir.FieldName, Ir.AggregateField)]) ->
      case NE.nonEmpty aggregateFields of
        Nothing -> refute (pure NoAggregatesMustBeABug)
        Just fields -> do
          aggregates <-
            traverse
              (\(fieldName, aggregateField) -> do
                 fmap
                   (\aliasedThing ->
                      Aliased {aliasedAlias = Ir.getFieldNameTxt fieldName, ..})
                   (fromAggregateField aggregateField))
              fields
          pure (AggregateFieldSource aggregates)
    Ir.TAFExp text ->
      pure
        (ExpressionFieldSource
           Aliased
             { aliasedThing = Tsql.ValueExpression (Odbc.TextValue text)
             , aliasedAlias = name
             })
    Ir.TAFNodes {} -> refute (pure (AggTypeUnsupportedForNow field))

fromAggregateField :: Ir.AggregateField -> ReaderT EntityAlias FromIr Aggregate
fromAggregateField aggregateField =
  case aggregateField of
    Ir.AFExp text -> pure (TextAggregate text)
    Ir.AFCount countType ->
      fmap
        CountAggregate
        (case countType of
           Sql.CTStar -> pure StarCountable
           Sql.CTSimple fields ->
             case NE.nonEmpty fields of
               Nothing -> refute (pure MalformedAgg)
               Just fields' -> do
                 fields'' <- traverse fromPGCol fields'
                 pure (NonNullFieldCountable fields'')
           Sql.CTDistinct fields ->
             case NE.nonEmpty fields of
               Nothing -> refute (pure MalformedAgg)
               Just fields' -> do
                 fields'' <- traverse fromPGCol fields'
                 pure (DistinctCountable fields''))
    Ir.AFOp Ir.AggregateOp {_aoOp = op, _aoFields = fields} -> do
      fs <- case NE.nonEmpty fields of
              Nothing -> refute (pure MalformedAgg)
              Just fs -> pure fs
      args <-
        traverse
          (\(_fieldName, pgColFld) ->
             case pgColFld of
               Ir.PCFCol pgCol -> fmap ColumnExpression (fromPGCol pgCol)
               Ir.PCFExp text -> pure (ValueExpression (Odbc.TextValue text)))
          fs
      pure (OpAggregate op args)

-- | The main sources of fields, either constants, fields or via joins.
fromAnnFieldsG ::
     Map Sql.QualifiedTable EntityAlias
  -> StringifyNumbers
  -> (Ir.FieldName, Ir.AnnFieldG Graphql.UnpreparedValue)
  -> ReaderT EntityAlias FromIr FieldSource
fromAnnFieldsG existingJoins stringifyNumbers (Ir.FieldName name, field) =
  case field of
    Ir.AFColumn annColumnField -> do
      expression <- fromAnnColumnField stringifyNumbers annColumnField
      pure
        (ExpressionFieldSource
           Aliased {aliasedThing = expression, aliasedAlias = name})
    Ir.AFExpression text ->
      pure
        (ExpressionFieldSource
           Aliased
             { aliasedThing = Tsql.ValueExpression (Odbc.TextValue text)
             , aliasedAlias = name
             })
    Ir.AFObjectRelation objectRelationSelectG ->
      fmap
        (\aliasedThing ->
           JoinFieldSource (Aliased {aliasedThing, aliasedAlias = name}))
        (fromObjectRelationSelectG existingJoins objectRelationSelectG)
    Ir.AFArrayRelation arraySelectG ->
      fmap
        (\aliasedThing ->
           JoinFieldSource (Aliased {aliasedThing, aliasedAlias = name}))
        (fromArraySelectG arraySelectG)
    -- TODO:
    -- Vamshi said to ignore these three for now:
    Ir.AFNodeId {} -> refute (pure (FieldTypeUnsupportedForNow field))
    Ir.AFRemote {} -> refute (pure (FieldTypeUnsupportedForNow field))
    Ir.AFComputedField {} -> refute (pure (FieldTypeUnsupportedForNow field))

-- | Here is where we project a field as a column expression. If
-- number stringification is on, then we wrap it in a
-- 'ToStringExpression' so that it's casted when being projected.
fromAnnColumnField ::
     StringifyNumbers
  -> Ir.AnnColumnField
  -> ReaderT EntityAlias FromIr Expression
fromAnnColumnField stringifyNumbers annColumnField = do
  fieldName <- fromPGCol pgCol
  if asText || (Ir.isScalarColumnWhere Sql.isBigNum typ && stringifyNumbers == StringifyNumbers)
     then pure (ToStringExpression (ColumnExpression fieldName))
     else pure (ColumnExpression fieldName)
  where
    Ir.AnnColumnField { _acfInfo = Ir.PGColumnInfo{pgiColumn=pgCol,pgiType=typ}
                      , _acfAsText = asText :: Bool
                      , _acfOp = _ :: Maybe Ir.ColumnOp -- TODO: What's this?
                      } = annColumnField

-- | This is where a field name "foo" is resolved to a fully qualified
-- field name [table].[foo]. The table name comes from EntityAlias in
-- the ReaderT.
fromPGCol :: Sql.PGCol -> ReaderT EntityAlias FromIr FieldName
fromPGCol pgCol = do
  EntityAlias {entityAliasText} <- ask
  pure (FieldName {fieldName = Sql.getPGColTxt pgCol, fieldNameEntity = entityAliasText})

fieldSourceProjections :: FieldSource -> NonEmpty Projection
fieldSourceProjections =
  \case
    ExpressionFieldSource aliasedExpression ->
      pure (ExpressionProjection aliasedExpression)
    JoinFieldSource aliasedJoin ->
      pure
        (ExpressionProjection
           (aliasedJoin
              { aliasedThing =
                  JsonQueryExpression
                    (ColumnExpression
                       (joinAliasToField
                          (joinJoinAlias (aliasedThing aliasedJoin))))
              }))
    AggregateFieldSource aggregates -> fmap AggregateProjection aggregates

joinAliasToField :: JoinAlias -> FieldName
joinAliasToField JoinAlias {..} =
  FieldName
    { fieldNameEntity = joinAliasEntity
    , fieldName = fromMaybe (error "TODO: Eliminate this case. joinAliasToField") joinAliasField
    }

fieldSourceJoin :: FieldSource -> Maybe Join
fieldSourceJoin =
  \case
    JoinFieldSource aliasedJoin -> pure (aliasedThing aliasedJoin)
    ExpressionFieldSource {} -> Nothing
    AggregateFieldSource {} -> Nothing

--------------------------------------------------------------------------------
-- Joins

fromObjectRelationSelectG ::
     Map Sql.QualifiedTable EntityAlias
  -> Ir.ObjectRelationSelectG Graphql.UnpreparedValue
  -> ReaderT EntityAlias FromIr Join
fromObjectRelationSelectG existingJoins annRelationSelectG = do
  eitherAliasOrFrom <- lift (lookupTableFrom existingJoins tableFrom)
  let entityAlias :: EntityAlias = either id fromAlias eitherAliasOrFrom
  fieldSources <-
    local
      (const entityAlias)
      (traverse (fromAnnFieldsG mempty LeaveNumbersAlone) fields)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> refute (pure NoProjectionFields)
      Just ne -> pure ne
  joinJoinAlias <-
    do fieldName <- lift (fromRelName aarRelationshipName)
       alias <- lift (generateEntityAlias (ObjectRelationTemplate fieldName))
       pure
         JoinAlias
           {joinAliasEntity = alias, joinAliasField = pure jsonFieldName}
  let selectFor = JsonFor JsonSingleton
  filterExpression <- local (const entityAlias) (fromAnnBoolExp tableFilter)
  case eitherAliasOrFrom of
    Right selectFrom -> do
      foreignKeyConditions <- fromMapping selectFrom mapping
      pure
        Join
          { joinJoinAlias
          , joinSource =
              JoinSelect
                Select
                  { selectOrderBy = Nothing
                  , selectTop = NoTop
                  , selectProjections
                  , selectFrom
                  , selectJoins = mapMaybe fieldSourceJoin fieldSources
                  , selectWhere =
                      Where (foreignKeyConditions <> [filterExpression])
                  , selectFor
                  , selectOffset = Nothing
                  }
          }
    Left _entityAlias ->
      pure
        Join
          { joinJoinAlias
          , joinSource =
              JoinReselect
                Reselect
                  { reselectProjections = selectProjections
                  , reselectFor = selectFor
                  , reselectWhere = Where [filterExpression]
                  }
          }
  where
    Ir.AnnObjectSelectG { _aosFields = fields :: Ir.AnnFieldsG Graphql.UnpreparedValue
                        , _aosTableFrom = tableFrom :: Sql.QualifiedTable
                        , _aosTableFilter = tableFilter :: Ir.AnnBoolExp Graphql.UnpreparedValue
                        } = annObjectSelectG
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annObjectSelectG :: Ir.AnnObjectSelectG Graphql.UnpreparedValue
                          } = annRelationSelectG

lookupTableFrom ::
     Map Sql.QualifiedTable EntityAlias
  -> Sql.QualifiedTable
  -> FromIr (Either EntityAlias From)
lookupTableFrom existingJoins tableFrom = do
  case M.lookup tableFrom existingJoins of
    Just entityAlias -> pure (Left entityAlias)
    Nothing -> fmap Right (fromQualifiedTable tableFrom)

fromArraySelectG :: Ir.ArraySelectG Graphql.UnpreparedValue -> ReaderT EntityAlias FromIr Join
fromArraySelectG =
  \case
    Ir.ASSimple arrayRelationSelectG ->
      fromArrayRelationSelectG arrayRelationSelectG
    Ir.ASAggregate arrayAggregateSelectG ->
      fromArrayAggregateSelectG arrayAggregateSelectG
    select@Ir.ASConnection {} ->
      refute (pure (UnsupportedArraySelect select))

fromArrayAggregateSelectG ::
     Ir.AnnRelationSelectG (Ir.AnnAggregateSelectG Graphql.UnpreparedValue)
  -> ReaderT EntityAlias FromIr Join
fromArrayAggregateSelectG annRelationSelectG = do
  fieldName <- lift (fromRelName aarRelationshipName)
  select <- lift (fromSelectAggregate annSelectG)
  joinSelect <-
    do foreignKeyConditions <- fromMapping (selectFrom select) mapping
       pure
         select {selectWhere = Where foreignKeyConditions <> selectWhere select}
  alias <- lift (generateEntityAlias (ArrayAggregateTemplate fieldName))
  pure
    Join
      { joinJoinAlias =
          JoinAlias
            {joinAliasEntity = alias, joinAliasField = pure jsonFieldName}
      , joinSource = JoinSelect joinSelect
      }
  where
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annSelectG
                          } = annRelationSelectG

fromArrayRelationSelectG :: Ir.ArrayRelationSelectG Graphql.UnpreparedValue -> ReaderT EntityAlias FromIr Join
fromArrayRelationSelectG annRelationSelectG = do
  fieldName <- lift (fromRelName aarRelationshipName)
  select <- lift (fromSelectRows annSelectG)
  joinSelect <-
    do foreignKeyConditions <- fromMapping (selectFrom select) mapping
       pure
         select {selectWhere = Where foreignKeyConditions <> selectWhere select}
  alias <- lift (generateEntityAlias (ArrayRelationTemplate fieldName))
  pure
    Join
      { joinJoinAlias =
          JoinAlias
            {joinAliasEntity = alias, joinAliasField = pure jsonFieldName}
      , joinSource = JoinSelect joinSelect
      }
  where
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annSelectG
                          } = annRelationSelectG

fromRelName :: Ir.RelName -> FromIr Text
fromRelName relName =
  pure (Ir.relNameToTxt relName)

-- | The context given by the reader is of the previous/parent
-- "remote" table. The WHERE that we're generating goes in the child,
-- "local" query. The @From@ passed in as argument is the local table.
--
-- We should hope to see e.g. "post.category = category.id" for a
-- local table of post and a remote table of category.
--
-- The left/right columns in @HashMap Sql.PGCol Sql.PGCol@ corresponds
-- to the left/right of @select ... join ...@. Therefore left=remote,
-- right=local in this context.
fromMapping ::
     From
  -> HashMap Sql.PGCol Sql.PGCol
  -> ReaderT EntityAlias FromIr [Expression]
fromMapping localFrom =
  traverse
    (\(remotePgCol, localPgCol) -> do
       localFieldName <- local (const (fromAlias localFrom)) (fromPGCol localPgCol)
       remoteFieldName <- fromPGCol remotePgCol
       pure
         (EqualExpression
            (ColumnExpression localFieldName)
            (ColumnExpression remoteFieldName))) .
  HM.toList

--------------------------------------------------------------------------------
-- Basic SQL expression types

fromOpExpG :: Expression -> Ir.OpExpG Graphql.UnpreparedValue -> FromIr Expression
fromOpExpG expression =
  \case
    Ir.ANISNULL -> pure (IsNullExpression expression)
    op -> (refute (pure (UnsupportedOpExpG op)))

fromSQLExpAsInt :: Sql.SQLExp -> FromIr Expression
fromSQLExpAsInt =
  \case
    s@(Sql.SELit text) ->
      case T.decimal text of
        Right (d, "") -> pure (ValueExpression (Odbc.IntValue d))
        _ -> refute (pure (InvalidIntegerishSql s))
    s -> refute (pure (InvalidIntegerishSql s))

fromGBoolExp :: Ir.GBoolExp Expression -> ReaderT EntityAlias FromIr Expression
fromGBoolExp =
  \case
    Ir.BoolAnd expressions ->
      fmap AndExpression (traverse fromGBoolExp expressions)
    Ir.BoolOr expressions ->
      fmap OrExpression (traverse fromGBoolExp expressions)
    Ir.BoolNot expression -> fmap NotExpression (fromGBoolExp expression)
    Ir.BoolExists gExists -> fmap ExistsExpression (fromGExists gExists)
    Ir.BoolFld expression -> pure expression

--------------------------------------------------------------------------------
-- Misc combinators

trueExpression :: Expression
trueExpression = ValueExpression (Odbc.BoolValue True)

--------------------------------------------------------------------------------
-- Constants

jsonFieldName :: Text
jsonFieldName = "json"

aggFieldName :: Text
aggFieldName = "agg"

existsFieldName :: Text
existsFieldName = "exists_placeholder"

--------------------------------------------------------------------------------
-- Name generation

data NameTemplate
  = ArrayRelationTemplate Text
  | ArrayAggregateTemplate Text
  | ObjectRelationTemplate Text
  | TableTemplate Text
  | ForOrderAlias Text

generateEntityAlias :: NameTemplate -> FromIr Text
generateEntityAlias template = do
  FromIr (modify' (M.insertWith (+) prefix start))
  i <- FromIr get
  pure (prefix <> T.pack (show (fromMaybe start (M.lookup prefix i))))
  where
    start = 1
    prefix = T.take 20 rendered
    rendered =
      case template of
        ArrayRelationTemplate sample -> "ar_" <> sample
        ArrayAggregateTemplate sample -> "aa_" <> sample
        ObjectRelationTemplate sample -> "or_" <> sample
        TableTemplate sample -> "t_" <> sample
        ForOrderAlias sample -> "order_" <> sample

fromAlias :: From -> EntityAlias
fromAlias (FromQualifiedTable Aliased {aliasedAlias}) = EntityAlias aliasedAlias
