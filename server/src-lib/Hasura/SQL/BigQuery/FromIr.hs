-- | Translate from the DML to the BigQuery dialect.
{-

Basic fields are selected as-is:

  Album { Title }

  =>

  SELECT AS STRUCT `t_Album1`.`Title` AS `Title`
  FROM `chinook`.`Album` AS `t_Album1`

Singleton object relations are selected via LEFT OUTER JOIN with an ON
for the mapping fields:

  Album { Artist { Name } }

  =>

  SELECT AS STRUCT (SELECT AS STRUCT `or_Artist1`.`Name` AS `Name`) AS `Artist`
  FROM `chinook`.`Album` AS `t_Album1`
  LEFT OUTER JOIN (SELECT AS STRUCT *
                   FROM `chinook`.`Artist` AS `t_Artist1`)
  AS `or_Artist1`
  ON (((`or_Artist1`.`ArtistId`) = (`t_Album1`.`ArtistId`)))

Array relations are selected via ARRAY() with a sub-select in it:

  Album { Track { Name } }

  =>

  SELECT AS STRUCT ARRAY((SELECT AS STRUCT `t_Track1`.`Name` AS `Name`
                          FROM `chinook`.`Track` AS `t_Track1`
                          WHERE ((`t_Track1`.`AlbumId`) = (`t_Album1`.`AlbumId`)))) AS `Track`
  FROM `chinook`.`Album` AS `t_Album1`

Each row is selected AS STRUCT which produces nested records in the
result.

Example of object relation within array relation:

  Album {
    Track { album { Title } }
  }

  =>

  SELECT AS STRUCT ARRAY((SELECT AS STRUCT (SELECT AS STRUCT `or_album1`.`Title` AS `Title`) AS `album`
                          FROM `chinook`.`Track` AS `t_Track1`
                          LEFT OUTER JOIN (SELECT AS STRUCT *
                                           FROM `chinook`.`Album` AS `t_Album2`)
                          AS `or_album1`
                          ON (((`or_album1`.`AlbumId`) = (`t_Track1`.`AlbumId`)))
                          WHERE ((`t_Track1`.`AlbumId`) = (`t_Album1`.`AlbumId`)))) AS `Track`
  FROM `chinook`.`Album` AS `t_Album1`

Aggregate query:

  Album_aggregate {
    aggregate{count}
  }

  =>

  SELECT AS STRUCT COUNT(*) AS `count`
  FROM `chinook`.`Album` AS `t_Album1`

Order by object relation:

  Album(order_by: {Artist: {Name: asc}}){
      Title
  }

  =>

  SELECT AS STRUCT `t_Album1`.`Title` AS `Title`
  FROM `chinook`.`Album` AS `t_Album1`
  LEFT OUTER JOIN (SELECT *
                   FROM `chinook`.`Artist` AS `t_Artist1`)
  AS `order_Artist1`
  ON (((`order_Artist1`.`ArtistId`) = (`t_Album1`.`ArtistId`)))
  ORDER BY `order_Artist1`.`Name` ASC NULLS LAST

Order by aggregation:

  Album(order_by: {Track_aggregate: {count: asc}}) {
    Title
  }

  =>

  SELECT AS STRUCT `t_Album1`.`Title` AS `Title`
  FROM `chinook`.`Album` AS `t_Album1`
  ORDER BY ((SELECT COUNT(*) AS `agg`
            FROM `chinook`.`Track` AS `t_Track1`
            WHERE ((`t_Track1`.`AlbumId`) = (`t_Album1`.`AlbumId`)))) ASC NULLS LAST

-}

module Hasura.SQL.BigQuery.FromIr
  ( fromSelectRows
  , mkSQLSelect
  , fromRootField
  , fromSelectAggregate
  , Error(..)
  , runFromIr
  , FromIr
  , jsonFieldName
  , trueExpression
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
import Data.Maybe
import           Data.Proxy
import           Data.Sequence (Seq)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Read as T
import qualified Database.ODBC.SQLServer as Odbc
import qualified Hasura.GraphQL.Context as Graphql
import qualified Hasura.RQL.DML.Select as DS
import qualified Hasura.RQL.DML.Select.Types as Ir
import qualified Hasura.RQL.Types.BoolExp as Ir
import qualified Hasura.RQL.Types.Column as Ir
import qualified Hasura.RQL.Types.Common as Ir
import qualified Hasura.RQL.Types.DML as Ir
import           Hasura.SQL.BigQuery.Types as BigQuery
import qualified Hasura.SQL.DML as Sql
import qualified Hasura.SQL.Types as Sql
import           Prelude

--------------------------------------------------------------------------------
-- Types

-- | Most of these errors should be checked for legitimacy.
data Error
  = FromTypeUnsupported (Ir.SelectFromG Expression)
  | NoOrderSpecifiedInOrderBy
  | MalformedAgg
  | FieldTypeUnsupportedForNow (Ir.AnnFieldG Expression)
  | AggTypeUnsupportedForNow (Ir.TableAggregateFieldG Expression)
  | NodesUnsupportedForNow (Ir.TableAggregateFieldG Expression)
  | NoProjectionFields
  | NoAggregatesMustBeABug
  | UnsupportedArraySelect (Ir.ArraySelectG Expression)
  | UnsupportedOpExpG (Ir.OpExpG Expression)
  | UnsupportedSQLExp Expression
  | UnsupportedDistinctOn
  | InvalidIntegerishSql Sql.SQLExp
  | DistinctIsn'tSupported
  | ConnectionsNotSupported
  | ActionsNotSupported
  | RootNotSupported
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
  -> Ir.AnnSelectG (Ir.AnnFieldsG Expression) Expression
  -> FromIr BigQuery.Select
mkSQLSelect jsonAggSelect annSimpleSel =
  case jsonAggSelect of
    Ir.JASMultipleRows -> fromSelectRows annSimpleSel
    Ir.JASSingleObject -> do
      select <- fromSelectRows annSimpleSel
      pure select {selectAsStruct = AsStruct, selectTop = Top 1}

-- | Convert from the IR database query into a select.
fromRootField ::
     Graphql.RootField (Graphql.QueryDB Expression) void1 void2 void3
  -> FromIr Select
fromRootField =
  \case
    Graphql.RFDB (Graphql.QDBPrimaryKey s) -> mkSQLSelect DS.JASSingleObject s
    Graphql.RFDB (Graphql.QDBSimple s) -> mkSQLSelect DS.JASMultipleRows s
    Graphql.RFDB (Graphql.QDBAggregation s) -> fromSelectAggregate s
    Graphql.RFDB (Graphql.QDBConnection {}) ->
      refute (pure ConnectionsNotSupported)
    Graphql.RFAction {} -> refute (pure ActionsNotSupported)
    _ -> refute (pure RootNotSupported)

--------------------------------------------------------------------------------
-- Top-level exported functions

fromSelectRows :: Ir.AnnSelectG (Ir.AnnFieldsG Expression) Expression -> FromIr BigQuery.Select
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
       } <- runReaderT (fromSelectArgsG args) (fromAlias selectFrom)
  fieldSources <-
    runReaderT
      (traverse (fromAnnFieldsG stringifyNumbers) fields)
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
      , selectAsStruct = AsStruct
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
     Ir.AnnSelectG [(Ir.FieldName, Ir.TableAggregateFieldG Expression)] Expression
  -> FromIr BigQuery.Select
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
      , selectAsStruct = AsStruct
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
  } deriving (Show)

data UnfurledJoin = UnfurledJoin
  { unfurledJoin :: Join
  , unfurledObjectTableAlias :: Maybe (Sql.QualifiedTable, EntityAlias)
    -- ^ Recorded if we joined onto an object relation.
  } deriving (Show)

fromSelectArgsG :: Ir.SelectArgsG Expression -> ReaderT EntityAlias FromIr Args
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
  pure
    Args
      { argsJoins = map unfurledJoin (toList joins)
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
     Ir.AnnOrderByItemG Expression -> WriterT (Seq UnfurledJoin) (ReaderT EntityAlias FromIr) OrderBy
fromAnnOrderByItemG Ir.OrderByItemG {obiType, obiColumn, obiNulls} = do
  orderByExpression <- unfurlAnnOrderByElement obiColumn
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
     Ir.AnnOrderByElement Expression -> WriterT (Seq UnfurledJoin) (ReaderT EntityAlias FromIr) Expression
unfurlAnnOrderByElement =
  \case
    Ir.AOCColumn pgColumnInfo -> do
      fieldName <- lift (fromPGColumnInfo pgColumnInfo)
      pure (ColumnExpression fieldName)
    Ir.AOCObjectRelation Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp annOrderByElementG -> do
      selectFrom <- lift (lift (fromQualifiedTable table))
      joinAliasEntity <-
        lift (lift (generateEntityAlias (ForOrderAlias (tableNameText table))))
      foreignKeyConditions <-
        lift (fromMapping (EntityAlias joinAliasEntity) mapping)
      whereExpression <-
        lift (local (const (fromAlias selectFrom)) (fromAnnBoolExp annBoolExp))
      tell
        (pure
           (UnfurledJoin
              { unfurledJoin =
                  LeftOuterJoin
                    { joinSource =
                        JoinSelect
                          Select
                            { selectTop = NoTop
                            , selectProjections = NE.fromList [StarProjection]
                            , selectFrom
                            , selectJoins = []
                            , selectWhere = Where [whereExpression]
                            , selectAsStruct = NoStruct
                            , selectOrderBy = Nothing
                            , selectOffset = Nothing
                            }
                    , joinJoinAlias =
                        JoinAlias
                          { joinAliasEntity -- , joinAliasField = Nothing
                          }
                    , joinOn = AndExpression foreignKeyConditions
                    , joinProjections = NE.fromList [StarProjection]
                    }
              , unfurledObjectTableAlias =
                  Just (table, EntityAlias joinAliasEntity)
              }))
      local
        (const (EntityAlias joinAliasEntity))
        (unfurlAnnOrderByElement annOrderByElementG)
    Ir.AOCArrayAggregation Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp annAggregateOrderBy -> do
      selectFrom <- lift (lift (fromQualifiedTable table))
      let alias = aggFieldName
      foreignKeyConditions <-
        lift (fromMapping (fromAlias selectFrom) mapping)
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
      pure
        (SelectExpression
           Select
             { selectTop = NoTop
             , selectProjections =
                 NE.fromList
                   [ AggregateProjection
                       Aliased {aliasedThing = aggregate, aliasedAlias = alias}
                   ]
             , selectFrom
             , selectJoins = []
             , selectWhere = Where (foreignKeyConditions <> [whereExpression])
             , selectAsStruct = NoStruct
             , selectOrderBy = Nothing
             , selectOffset = Nothing
             })

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
              TableName {tableName = qname, tableNameSchema = "chinook"{-schemaName-}}
          , aliasedAlias = alias
          }))
  where
    Sql.QualifiedObject { qSchema = Sql.SchemaName schemaName
                         -- TODO: Consider many x.y.z. in schema name.
                        , qName = Sql.TableName qname
                        } = qualifiedObject

fromAnnBoolExp ::
     Ir.GBoolExp (Ir.AnnBoolExpFld Expression)
  -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExp = traverse fromAnnBoolExpFld >=> fromGBoolExp

fromAnnBoolExpFld ::
     Ir.AnnBoolExpFld Expression -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExpFld =
  \case
    Ir.AVCol pgColumnInfo opExpGs -> do
      expression <- fmap ColumnExpression (fromPGColumnInfo pgColumnInfo)
      expressions <- traverse (lift . fromOpExpG expression) opExpGs
      pure (AndExpression expressions)
    Ir.AVRel Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp -> do
      selectFrom <- lift (fromQualifiedTable table)
      foreignKeyConditions <- fromMapping (fromAlias selectFrom) mapping
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
             , selectAsStruct = NoStruct
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
      , selectAsStruct = NoStruct
      , selectOffset = Nothing
      }

--------------------------------------------------------------------------------
-- Sources of projected fields
--
-- Because in the IR, a field projected can be a foreign object, we
-- have to both generate a projection AND on the side generate a join.
--
-- So a @FieldSource@ couples the idea of the projected thing and the
-- source of it (via 'Aliased').

data FieldSource
  = ExpressionFieldSource (Aliased Expression)
  | JoinFieldSource (Aliased Join)
  | AggregateFieldSource (NonEmpty (Aliased Aggregate))
  deriving (Eq, Show)

fromTableAggregateFieldG ::
     (Ir.FieldName, Ir.TableAggregateFieldG Expression) -> ReaderT EntityAlias FromIr FieldSource
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
             { aliasedThing = BigQuery.ValueExpression (Odbc.TextValue text)
             , aliasedAlias = name
             })
    Ir.TAFNodes {} -> refute (pure (NodesUnsupportedForNow field))

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
     StringifyNumbers
  -> (Ir.FieldName, Ir.AnnFieldG Expression)
  -> ReaderT EntityAlias FromIr FieldSource
fromAnnFieldsG stringifyNumbers (Ir.FieldName name, field) =
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
             { aliasedThing = BigQuery.ValueExpression (Odbc.TextValue text)
             , aliasedAlias = name
             })
    Ir.AFObjectRelation objectRelationSelectG ->
      fmap
        (\aliasedThing ->
           JoinFieldSource (Aliased {aliasedThing, aliasedAlias = name}))
        (fromObjectRelationSelectG objectRelationSelectG)
    Ir.AFArrayRelation arraySelectG ->
      fmap
        (\case
           Left select ->
             ExpressionFieldSource
               Aliased
                 { aliasedThing = ArrayExpression (SelectExpression select)
                 , aliasedAlias = name
                 }
           Right aliasedThing ->
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
                  ReselectExpression
                    Reselect
                      { reselectProjections = joinProjections (aliasedThing aliasedJoin)
                      , reselectWhere = Where []
                      , reselectAsStruct = AsStruct
                      }
              }))
    AggregateFieldSource aggregates -> fmap AggregateProjection aggregates

fieldSourceJoin :: FieldSource -> Maybe Join
fieldSourceJoin =
  \case
    JoinFieldSource aliasedJoin -> pure (aliasedThing aliasedJoin)
    ExpressionFieldSource {} -> Nothing
    AggregateFieldSource {} -> Nothing

--------------------------------------------------------------------------------
-- Joins

fromObjectRelationSelectG ::
     Ir.ObjectRelationSelectG Expression
  -> ReaderT EntityAlias FromIr Join
fromObjectRelationSelectG annRelationSelectG = do
  joinJoinAlias <-
    do fieldName <- lift (fromRelName aarRelationshipName)
       alias <- lift (generateEntityAlias (ObjectRelationTemplate fieldName))
       pure
         JoinAlias
           { joinAliasEntity = alias -- , joinAliasField = pure jsonFieldName
           }
  selectFrom <- lift (fromQualifiedTable tableFrom)
  let entityAlias :: EntityAlias = fromAlias selectFrom
  filterExpression <- local (const entityAlias) (fromAnnBoolExp tableFilter)
  fieldSources <-
    local
      (const (EntityAlias (joinAliasEntity joinJoinAlias)))
      (traverse (fromAnnFieldsG LeaveNumbersAlone) fields)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> refute (pure NoProjectionFields)
      Just ne -> pure ne
  foreignKeyConditions <-
    fromMapping (EntityAlias (joinAliasEntity joinJoinAlias)) mapping
  pure
    LeftOuterJoin
      { joinJoinAlias
      , joinSource =
          JoinSelect
            Select
              { selectOrderBy = Nothing
              , selectTop = NoTop
              , selectProjections = NE.fromList [StarProjection]
              , selectFrom
              , selectJoins = mapMaybe fieldSourceJoin fieldSources
              , selectWhere = Where [filterExpression]
              , selectAsStruct = AsStruct
              , selectOffset = Nothing
              }
      , joinOn = AndExpression foreignKeyConditions
      , joinProjections = selectProjections
      }
  where
    Ir.AnnObjectSelectG { _aosFields = fields :: Ir.AnnFieldsG Expression
                        , _aosTableFrom = tableFrom :: Sql.QualifiedTable
                        , _aosTableFilter = tableFilter :: Ir.AnnBoolExp Expression
                        } = annObjectSelectG
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annObjectSelectG :: Ir.AnnObjectSelectG Expression
                          } = annRelationSelectG

fromArraySelectG :: Ir.ArraySelectG Expression -> ReaderT EntityAlias FromIr (Either Select Join)
fromArraySelectG =
  \case
    Ir.ASSimple arrayRelationSelectG ->
      fmap Left (fromArrayRelationSelectG arrayRelationSelectG)
    Ir.ASAggregate arrayAggregateSelectG ->
      fmap Right (fromArrayAggregateSelectG arrayAggregateSelectG)
    select@Ir.ASConnection {} ->
      refute (pure (UnsupportedArraySelect select))

fromArrayAggregateSelectG ::
     Ir.AnnRelationSelectG (Ir.AnnAggregateSelectG Expression)
  -> ReaderT EntityAlias FromIr Join
fromArrayAggregateSelectG annRelationSelectG = do
  fieldName <- lift (fromRelName aarRelationshipName)
  select <- lift (fromSelectAggregate annSelectG)
  foreignKeyConditions <- fromMapping (fromAlias (selectFrom select)) mapping
  joinSelect <- pure select {selectWhere = selectWhere select}
  alias <- lift (generateEntityAlias (ArrayAggregateTemplate fieldName))
  pure
    LeftOuterJoin
      { joinJoinAlias =
          JoinAlias
            {joinAliasEntity = alias-- , joinAliasField = pure jsonFieldName
            }
      , joinSource = JoinSelect joinSelect { selectProjections = NE.fromList [StarProjection]}
      , joinOn = AndExpression foreignKeyConditions
      , joinProjections = selectProjections select -- TODO: these are in the wrong scope -- should be join alias.
      }
  where
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annSelectG
                          } = annRelationSelectG

fromArrayRelationSelectG :: Ir.ArrayRelationSelectG Expression -> ReaderT EntityAlias FromIr Select
fromArrayRelationSelectG annRelationSelectG = do
  {-fieldName <- lift (fromRelName aarRelationshipName)-}
  select <- lift (fromSelectRows annSelectG)
  joinSelect <-
    do foreignKeyConditions <- fromMapping (fromAlias (selectFrom select)) mapping
       pure
         select {selectWhere = Where foreignKeyConditions <> selectWhere select}
  {-alias <- lift (generateEntityAlias (ArrayRelationTemplate fieldName))-}
  pure
    joinSelect
    {-Join
      { joinJoinAlias =
          JoinAlias
            {joinAliasEntity = alias, joinAliasField = pure jsonFieldName}
      , joinSource = JoinSelect joinSelect
      }-}
  where
    Ir.AnnRelationSelectG { aarRelationshipName = _
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
     EntityAlias
  -> HashMap Sql.PGCol Sql.PGCol
  -> ReaderT EntityAlias FromIr [Expression]
fromMapping alias =
  traverse
    (\(remotePgCol, localPgCol) -> do
       localFieldName <- local (const alias) (fromPGCol localPgCol)
       remoteFieldName <- fromPGCol remotePgCol
       pure
         (EqualExpression
            (ColumnExpression localFieldName)
            (ColumnExpression remoteFieldName))) .
  HM.toList

--------------------------------------------------------------------------------
-- Basic SQL expression types

fromOpExpG :: Expression -> Ir.OpExpG Expression -> FromIr Expression
fromOpExpG expression op =
  case op of
    Ir.ANISNULL                  -> pure (IsNullExpression expression)
    Ir.ANISNOTNULL               -> pure (IsNotNullExpression expression)
    Ir.AEQ False val             -> pure (nullableBoolEquality expression val)
    Ir.AEQ True val              -> pure (EqualExpression expression val)
    Ir.ANE False val             -> pure (nullableBoolInequality expression val)
    Ir.ANE True val              -> pure (NotEqualExpression expression val)
    Ir.AGT val                   -> pure (OpExpression MoreOp expression val)
    Ir.ALT val                   -> pure (OpExpression LessOp expression val)
    Ir.AGTE val                  -> pure (OpExpression MoreOrEqualOp expression val)
    Ir.ALTE val                  -> pure (OpExpression LessOrEqualOp expression val)
    Ir.ACast _casts              -> refute (pure (UnsupportedOpExpG op)) -- mkCastsExp casts
    Ir.AIN _val                  -> refute (pure (UnsupportedOpExpG op)) -- S.BECompareAny S.SEQ lhs val
    Ir.ANIN _val                 -> refute (pure (UnsupportedOpExpG op)) -- S.BENot $ S.BECompareAny S.SEQ lhs val
    Ir.ALIKE _val                -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SLIKE lhs val
    Ir.ANLIKE _val               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SNLIKE lhs val
    Ir.AILIKE _val               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SILIKE lhs val
    Ir.ANILIKE _val              -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SNILIKE lhs val
    Ir.ASIMILAR _val             -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SSIMILAR lhs val
    Ir.ANSIMILAR _val            -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SNSIMILAR lhs val
    Ir.AContains _val            -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SContains lhs val
    Ir.AContainedIn _val         -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SContainedIn lhs val
    Ir.AHasKey _val              -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SHasKey lhs val
    Ir.AHasKeysAny _val          -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SHasKeysAny lhs val
    Ir.AHasKeysAll _val          -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SHasKeysAll lhs val
    Ir.ASTContains _val          -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Contains" val
    Ir.ASTCrosses _val           -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Crosses" val
    Ir.ASTEquals _val            -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Equals" val
    Ir.ASTIntersects _val        -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Intersects" val
    Ir.ASTOverlaps _val          -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Overlaps" val
    Ir.ASTTouches _val           -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Touches" val
    Ir.ASTWithin _val            -> refute (pure (UnsupportedOpExpG op)) -- mkGeomOpBe "ST_Within" val
    Ir.ASTDWithinGeom {}         -> refute (pure (UnsupportedOpExpG op)) -- applySQLFn "ST_DWithin" [lhs, val, r]
    Ir.ASTDWithinGeog {}         -> refute (pure (UnsupportedOpExpG op)) -- applySQLFn "ST_DWithin" [lhs, val, r, sph]
    Ir.ASTIntersectsRast _val    -> refute (pure (UnsupportedOpExpG op)) -- applySTIntersects [lhs, val]
    Ir.ASTIntersectsNbandGeom {} -> refute (pure (UnsupportedOpExpG op)) -- applySTIntersects [lhs, nband, geommin]
    Ir.ASTIntersectsGeomNband {} -> refute (pure (UnsupportedOpExpG op)) -- applySTIntersects [lhs, geommin, withSQLNull mNband]
    Ir.CEQ _rhsCol               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SEQ lhs $ mkQCol rhsCol
    Ir.CNE _rhsCol               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SNE lhs $ mkQCol rhsCol
    Ir.CGT _rhsCol               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SGT lhs $ mkQCol rhsCol
    Ir.CLT _rhsCol               -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SLT lhs $ mkQCol rhsCol
    Ir.CGTE _rhsCol              -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SGTE lhs $ mkQCol rhsCol
    Ir.CLTE _rhsCol              -> refute (pure (UnsupportedOpExpG op)) -- S.BECompare S.SLTE lhs $ mkQCol rhsCol

nullableBoolEquality :: Expression -> Expression -> Expression
nullableBoolEquality x y =
  OrExpression
    [ EqualExpression x y
    , AndExpression [IsNullExpression x, IsNullExpression y]
    ]

nullableBoolInequality :: Expression -> Expression -> Expression
nullableBoolInequality x y =
  OrExpression
    [ NotEqualExpression x y
    , AndExpression [IsNotNullExpression x, IsNullExpression y]
    ]

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
fromAlias (FromOpenJson Aliased {aliasedAlias}) = EntityAlias aliasedAlias
