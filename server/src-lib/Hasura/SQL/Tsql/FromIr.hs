-- | Translate from the DML to the TSql dialect.

module Hasura.SQL.Tsql.FromIr
  ( fromSelectRows
  , fromSelectAggregate
  , Error(..)
  , runFromIr
  , FromIr
  ) where

import           Control.Monad
import           Control.Monad.Trans
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.State.Strict
import           Control.Monad.Validate
import           Data.Foldable
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Maybe
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Database.ODBC.SQLServer as Odbc
import qualified Hasura.RQL.DML.Select.Types as Ir
import qualified Hasura.RQL.Types.BoolExp as Ir
import qualified Hasura.RQL.Types.Column as Ir
import qualified Hasura.RQL.Types.Common as Ir
import qualified Hasura.SQL.DML as Sql
import           Hasura.SQL.Tsql.Types as Tsql
import qualified Hasura.SQL.Types as Sql
import           Prelude

--------------------------------------------------------------------------------
-- Types

data Error
  = FromTypeUnsupported (Ir.SelectFromG Sql.SQLExp)
  | MalformedAgg
  | FieldTypeUnsupportedForNow (Ir.AnnFieldG Sql.SQLExp)
  | AggTypeUnsupportedForNow (Ir.TableAggregateFieldG Sql.SQLExp)
  | NoProjectionFields
  | NoAggregatesMustBeABug
  | UnsupportedArraySelect (Ir.ArraySelectG Sql.SQLExp)
  deriving (Show, Eq)

newtype FromIr a = FromIr
  { unFromIr :: StateT (Map Text Int) (Validate (NonEmpty Error)) a
  } deriving (Functor, Applicative, Monad)

--------------------------------------------------------------------------------
-- Runners

runFromIr :: FromIr a -> Validate (NonEmpty Error) a
runFromIr fromIr = evalStateT (unFromIr fromIr) mempty

--------------------------------------------------------------------------------
-- Top-level exported functions

fromSelectRows :: Ir.AnnSelectG (Ir.AnnFieldsG Sql.SQLExp) Sql.SQLExp -> FromIr Tsql.Select
fromSelectRows annSelectG
  -- Here is a spot where the from'd thing binds a scope that the
  -- order/where will be related to.
 = do
  selectFrom <-
    case _asnFrom of
      Ir.FromTable qualifiedObject -> fromQualifiedTable qualifiedObject
      _ -> FromIr (refute (pure (FromTypeUnsupported _asnFrom)))
  fieldSources <- runReaderT (traverse fromAnnFieldsG _asnFields) (fromAlias selectFrom)
  filterExpression <- runReaderT (fromAnnBoolExp permFilter) (fromAlias selectFrom)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> FromIr (refute (pure NoProjectionFields))
      Just ne -> pure ne
  pure
    Select
      { selectTop =
          case mPermLimit of
            Nothing -> uncommented NoTop
            Just limit ->
              Commented
                { commentedComment = pure DueToPermission
                , commentedThing = Top limit
                }
      , selectProjections
      , selectFrom
      , selectJoins = mapMaybe fieldSourceJoin fieldSources
      , selectWhere = Where [filterExpression]
      , selectFor = JsonFor JsonArray
      }
  where
    Ir.AnnSelectG {_asnFields, _asnFrom, _asnPerm, _asnArgs, _asnStrfyNum} =
      annSelectG
    Ir.TablePerm {_tpLimit = mPermLimit, _tpFilter = permFilter} = _asnPerm

fromSelectAggregate ::
     Ir.AnnSelectG [(Ir.FieldName, Ir.TableAggregateFieldG Sql.SQLExp)] Sql.SQLExp
  -> FromIr Tsql.Select
fromSelectAggregate annSelectG = do
  selectFrom <-
    case _asnFrom of
      Ir.FromTable qualifiedObject -> fromQualifiedTable qualifiedObject
      _ -> FromIr (refute (pure (FromTypeUnsupported _asnFrom)))
  fieldSources <-
    runReaderT
      (traverse fromTableAggregateFieldG _asnFields)
      (fromAlias selectFrom)
  filterExpression <-
    runReaderT (fromAnnBoolExp permFilter) (fromAlias selectFrom)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> FromIr (refute (pure NoProjectionFields))
      Just ne -> pure ne
  pure
    Select
      { selectTop =
          case mPermLimit of
            Nothing -> uncommented NoTop
            Just limit ->
              Commented
                { commentedComment = pure DueToPermission
                , commentedThing = Top limit
                }
      , selectProjections
      , selectFrom
      , selectJoins = mapMaybe fieldSourceJoin fieldSources
      , selectWhere = Where [filterExpression]
      , selectFor = JsonFor JsonSingleton
      }
  where
    Ir.AnnSelectG {_asnFields, _asnFrom, _asnPerm, _asnArgs, _asnStrfyNum} =
      annSelectG
    Ir.TablePerm {_tpLimit = mPermLimit, _tpFilter = permFilter} = _asnPerm

--------------------------------------------------------------------------------
-- Conversion functions

fromAnnBoolExp ::
     Ir.GBoolExp (Ir.AnnBoolExpFld Sql.SQLExp)
  -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExp = traverse fromAnnBoolExpFld >=> fromGBoolExp

fromAnnBoolExpFld ::
     Ir.AnnBoolExpFld Sql.SQLExp -> ReaderT EntityAlias FromIr Expression
fromAnnBoolExpFld =
  \case
    Ir.AVCol pgColumnInfo opExpGs -> do
      expression <- fromPGColumnInfo pgColumnInfo
      expressions <- traverse (lift . fromOpExpG expression) opExpGs
      pure (AndExpression expressions)
    Ir.AVRel Ir.RelInfo {riMapping = mapping, riRTable = table} annBoolExp -> do
      selectFrom <- lift (fromQualifiedTable table)
      foreignKeyConditions <-
        traverse
          (\(from, to) -> do
             fromFieldName <- fromPGCol from
             toFieldName <- local (const (fromAlias selectFrom)) (fromPGCol to)
             pure
               (EqualExpression
                  (ColumnExpression fromFieldName)
                  (ColumnExpression toFieldName)))
          (HM.toList mapping)
      whereExpression <-
        local (const (fromAlias selectFrom)) (fromAnnBoolExp annBoolExp)
      pure
        (ExistsExpression
           Select
             { selectProjections =
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
             , selectTop = uncommented NoTop
             , selectFor = NoFor
             })

fromPGColumnInfo :: Ir.PGColumnInfo -> ReaderT EntityAlias FromIr Expression
fromPGColumnInfo info = do
  name <- fromPGColumnName' info
  pure (ColumnExpression name)

fromPGColumnName' :: Ir.PGColumnInfo -> ReaderT EntityAlias FromIr FieldName
fromPGColumnName' Ir.PGColumnInfo {pgiColumn = pgCol} = do
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
      { selectProjections =
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
      , selectTop = uncommented NoTop
      , selectFor = NoFor
      }

fromQualifiedTable :: Sql.QualifiedObject Sql.TableName -> FromIr From
fromQualifiedTable qualifiedObject = do
  alias <- generateEntityAlias tableAliasName
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

--------------------------------------------------------------------------------
-- Sources of projected fields

data FieldSource
  = ExpressionFieldSource (Aliased Expression)
  | ColumnFieldSource (Aliased FieldName)
  | JoinFieldSource (Aliased Join)
  | AggregateFieldSource (NonEmpty (Aliased Aggregate))
  deriving (Eq, Show)

fromTableAggregateFieldG ::
     (Ir.FieldName, Ir.TableAggregateFieldG Sql.SQLExp) -> ReaderT EntityAlias FromIr FieldSource
fromTableAggregateFieldG (Ir.FieldName name, field) =
  case field of
    Ir.TAFAgg (aggregateFields :: [(Ir.FieldName, Ir.AggregateField)]) ->
      case NE.nonEmpty aggregateFields of
        Nothing -> lift (FromIr (refute (pure NoAggregatesMustBeABug)))
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
    Ir.TAFNodes {} -> lift (FromIr (refute (pure (AggTypeUnsupportedForNow field))))

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
               Nothing -> lift (FromIr (refute (pure MalformedAgg)))
               Just fields' -> do
                 fields'' <- traverse fromPGCol fields'
                 pure (NonNullFieldCountable fields'')
           Sql.CTDistinct fields ->
             case NE.nonEmpty fields of
               Nothing -> lift (FromIr (refute (pure MalformedAgg)))
               Just fields' -> do
                 fields'' <- traverse fromPGCol fields'
                 pure (DistinctCountable fields''))
    Ir.AFOp Ir.AggregateOp{_aoOp,_aoFields} ->
      error "Ir.AFOp Ir.AggregateOp"

-- | The main sources of fields, either constants, fields or via joins.
fromAnnFieldsG :: (Ir.FieldName, Ir.AnnFieldG Sql.SQLExp) -> ReaderT EntityAlias FromIr FieldSource
fromAnnFieldsG (Ir.FieldName name, field) =
  case field of
    Ir.AFColumn annColumnField -> do
      fieldName <- fromAnnColumnField annColumnField
      pure
        (ColumnFieldSource
           Aliased
             { aliasedThing = fieldName
             , aliasedAlias = name
             })
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
           JoinFieldSource
             (Aliased
                {aliasedThing, aliasedAlias = name}))
        (fromObjectRelationSelectG objectRelationSelectG)
    Ir.AFArrayRelation arraySelectG ->
      fmap
        (\aliasedThing ->
           JoinFieldSource
             (Aliased
                {aliasedThing, aliasedAlias = name}))
        (fromArraySelectG arraySelectG)
    -- TODO:
    -- Vamshi said to ignore these three for now:
    Ir.AFNodeId {} -> lift (FromIr (refute (pure (FieldTypeUnsupportedForNow field))))
    Ir.AFRemote {} -> lift (FromIr (refute (pure (FieldTypeUnsupportedForNow field))))
    Ir.AFComputedField {} ->
      lift (FromIr (refute (pure (FieldTypeUnsupportedForNow field))))

fromAnnColumnField :: Ir.AnnColumnField -> ReaderT EntityAlias FromIr FieldName
fromAnnColumnField annColumnField = fromPGColumnName pgColumnInfo
  where
    Ir.AnnColumnField { _acfInfo = pgColumnInfo
                      , _acfAsText = _ :: Bool -- TODO: What's this?
                      , _acfOp = _ :: Maybe Ir.ColumnOp -- TODO: What's this?
                      } = annColumnField

fromPGColumnName :: Ir.PGColumnInfo -> ReaderT EntityAlias FromIr FieldName
fromPGColumnName Ir.PGColumnInfo{pgiColumn = pgCol} =
  fromPGCol pgCol

fromPGCol :: Sql.PGCol -> ReaderT EntityAlias FromIr FieldName
fromPGCol pgCol = do
  EntityAlias {entityAliasText} <- ask
  pure (FieldName {fieldName = Sql.getPGColTxt pgCol, fieldNameEntity = entityAliasText})

fieldSourceProjections :: FieldSource -> NonEmpty Projection
fieldSourceProjections =
  \case
    ExpressionFieldSource aliasedExpression ->
      pure (ExpressionProjection aliasedExpression)
    ColumnFieldSource name -> pure (FieldNameProjection name)
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
  FieldName {fieldNameEntity = joinAliasEntity, fieldName = joinAliasField}

fieldSourceJoin :: FieldSource -> Maybe Join
fieldSourceJoin =
  \case
    JoinFieldSource aliasedJoin -> pure (aliasedThing aliasedJoin)
    ExpressionFieldSource {} -> Nothing
    ColumnFieldSource {} -> Nothing
    AggregateFieldSource {} -> Nothing

--------------------------------------------------------------------------------
-- Joins

fromObjectRelationSelectG :: Ir.ObjectRelationSelectG Sql.SQLExp -> ReaderT EntityAlias FromIr Join
fromObjectRelationSelectG annRelationSelectG = do
  selectFrom <- lift (fromQualifiedTable tableFrom)
  fieldSources <-
    local (const ((fromAlias selectFrom))) (traverse fromAnnFieldsG fields)
  filterExpression <-
    local (const (fromAlias selectFrom)) (fromAnnBoolExp tableFilter)
  selectProjections <-
    case NE.nonEmpty (concatMap (toList . fieldSourceProjections) fieldSources) of
      Nothing -> lift (FromIr (refute (pure NoProjectionFields)))
      Just ne -> pure ne
  _fieldName <- lift (fromRelName aarRelationshipName) -- TODO: Maybe use later for more readable names.
  foreignKeyConditions <-
    traverse
      (\(from, to) -> do
         fromFieldName <- fromPGCol from
         toFieldName <- local (const (fromAlias selectFrom)) (fromPGCol to)
         pure
           (EqualExpression
              (ColumnExpression fromFieldName)
              (ColumnExpression toFieldName)))
      (HM.toList mapping)
  alias <- lift (generateEntityAlias objectRelationAlias)
  pure
    Join
      { joinJoinAlias =
          JoinAlias {joinAliasEntity = alias, joinAliasField = jsonFieldName}
      , joinSelect =
          Select
            { selectTop = uncommented NoTop
            , selectProjections
            , selectFrom
            , selectJoins = mapMaybe fieldSourceJoin fieldSources
            , selectWhere = Where (foreignKeyConditions <> [filterExpression])
            , selectFor = JsonFor JsonSingleton
            }
      }
  where
    Ir.AnnObjectSelectG { _aosFields = fields :: Ir.AnnFieldsG Sql.SQLExp
                        , _aosTableFrom = tableFrom :: Sql.QualifiedTable
                        , _aosTableFilter = tableFilter :: Ir.AnnBoolExp Sql.SQLExp
                        } = annObjectSelectG
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annObjectSelectG :: Ir.AnnObjectSelectG Sql.SQLExp
                          } = annRelationSelectG

fromArraySelectG :: Ir.ArraySelectG Sql.SQLExp -> ReaderT EntityAlias FromIr Join
fromArraySelectG =
  \case
    Ir.ASSimple arrayRelationSelectG ->
      fromArrayRelationSelectG arrayRelationSelectG
    Ir.ASAggregate arrayAggregateSelectG ->
      fromArrayAggregateSelectG arrayAggregateSelectG
    select@Ir.ASConnection {} ->
      lift (FromIr (refute (pure (UnsupportedArraySelect select))))

fromArrayAggregateSelectG ::
     Ir.AnnRelationSelectG (Ir.AnnAggregateSelectG Sql.SQLExp)
  -> ReaderT EntityAlias FromIr Join
fromArrayAggregateSelectG annRelationSelectG = do
  _fieldName <- lift (fromRelName aarRelationshipName) -- TODO: use it?
  select <- lift (fromSelectAggregate annSelectG)
  joinSelect <-
    do foreignKeyConditions <-
         traverse
           (\(from, to) -> do
              fromFieldName <- fromPGCol from
              toFieldName <-
                local (const (fromAlias (selectFrom select))) (fromPGCol to)
              pure
                (EqualExpression
                   (ColumnExpression fromFieldName)
                   (ColumnExpression toFieldName)))
           (HM.toList mapping)
       pure
         select {selectWhere = Where foreignKeyConditions <> selectWhere select}
  alias <- lift (generateEntityAlias arrayAggregateName)
  pure
    Join
      { joinJoinAlias =
          JoinAlias {joinAliasEntity = alias, joinAliasField = jsonFieldName}
      , joinSelect
      }
  where
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annSelectG
                          } = annRelationSelectG

fromArrayRelationSelectG :: Ir.ArrayRelationSelectG Sql.SQLExp -> ReaderT EntityAlias FromIr Join
fromArrayRelationSelectG annRelationSelectG = do
  _fieldName <- lift (fromRelName aarRelationshipName) -- TODO: use it?
  select <- lift (fromSelectRows annSelectG)
  joinSelect <-
    do foreignKeyConditions <-
         traverse
           (\(from, to) -> do
              fromFieldName <- fromPGCol from
              toFieldName <-
                local (const (fromAlias (selectFrom select))) (fromPGCol to)
              pure
                (EqualExpression
                   (ColumnExpression fromFieldName)
                   (ColumnExpression toFieldName)))
           (HM.toList mapping)
       pure
         select {selectWhere = Where foreignKeyConditions <> selectWhere select}
  alias <- lift (generateEntityAlias arrayRelationAlias)
  pure
    Join
      { joinJoinAlias = JoinAlias {joinAliasEntity = alias, joinAliasField = jsonFieldName}
      , joinSelect
      }
  where
    Ir.AnnRelationSelectG { aarRelationshipName
                          , aarColumnMapping = mapping :: HashMap Sql.PGCol Sql.PGCol
                          , aarAnnSelect = annSelectG
                          } = annRelationSelectG

fromRelName :: Ir.RelName -> FromIr Text
fromRelName relName =
  pure (Ir.relNameToTxt relName)

--------------------------------------------------------------------------------
-- Basic SQL expression types

fromOpExpG :: Expression -> Ir.OpExpG Sql.SQLExp -> FromIr Expression
fromOpExpG expression =
  \case
    Ir.ANISNULL -> pure (IsNullExpression expression)

fromSQLExp :: Sql.SQLExp -> FromIr Expression
fromSQLExp =
  \case
    Sql.SENull -> pure (ValueExpression Odbc.NullValue)

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
-- Comments

uncommented :: a -> Commented a
uncommented a = Commented {commentedComment = Nothing, commentedThing = a}

--------------------------------------------------------------------------------
-- Misc combinators

trueExpression :: Expression
trueExpression = ValueExpression (Odbc.BoolValue True)

--------------------------------------------------------------------------------
-- Constants

jsonFieldName :: Text
jsonFieldName = "json"

existsFieldName :: Text
existsFieldName = "exists_placeholder"

arrayRelationAlias :: Text
arrayRelationAlias = "array_relation"

arrayAggregateName :: Text
arrayAggregateName = "array_aggregate_relation"

objectRelationAlias :: Text
objectRelationAlias = "object_relation"

tableAliasName :: Text
tableAliasName = "table"

--------------------------------------------------------------------------------
-- Name generation

generateEntityAlias :: Text -> FromIr Text
generateEntityAlias prefix = do
  FromIr (modify' (M.insertWith (+) prefix start))
  i <- FromIr get
  pure (prefix <> T.pack (show (fromMaybe start (M.lookup prefix i))))
  where start = 1

fromAlias :: From -> EntityAlias
fromAlias (FromQualifiedTable Aliased {aliasedAlias}) = EntityAlias aliasedAlias
