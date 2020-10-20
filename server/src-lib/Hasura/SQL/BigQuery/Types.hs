-- | Types for standard SQL; the language of BigQuery.

module Hasura.SQL.BigQuery.Types where

import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Text (Text)
import           Prelude

data Select = Select
  { selectTop :: !Top
  , selectProjections :: !(NonEmpty Projection)
  , selectFrom :: !From
  , selectJoins :: ![Join]
  , selectWhere :: !Where
  , selectAsStruct :: !AsStruct
  , selectOrderBy :: !(Maybe (NonEmpty OrderBy))
  , selectOffset :: !(Maybe Expression)
  , selectAsJson :: !AsJson
  } deriving (Eq, Show)

data AsStruct
  = NoStruct
  | AsStruct
  deriving (Eq, Show)

data AsJson
  = NoJson
  | AsJsonSingleton
  | AsJsonArray
  deriving (Eq, Show)

data Reselect = Reselect
  { reselectProjections :: !(NonEmpty Projection)
  , reselectWhere :: !Where
  , reselectAsStruct :: !AsStruct
  } deriving (Eq, Show)

data OrderBy = OrderBy
  { orderByExpression :: Expression
  , orderByOrder :: Order
  , orderByNullsOrder :: NullsOrder
  } deriving (Eq, Show)

data Order
  = AscOrder
  | DescOrder
  deriving (Eq, Show)

data NullsOrder
  = NullsFirst
  | NullsLast
  | NullsAnyOrder
  deriving (Eq, Show)

data Root
  = NoRoot
  | Root Text
  deriving (Eq, Show)

data JsonCardinality
  = JsonArray
  | JsonSingleton
  deriving (Eq, Show)

data Projection
  = ExpressionProjection (Aliased Expression)
  | FieldNameProjection (Aliased FieldName)
  | AggregateProjection (Aliased Aggregate)
  | StarProjection
  deriving (Eq, Show)

data Join = LeftOuterJoin
  { joinSource :: !JoinSource
  , joinJoinAlias :: !JoinAlias
  , joinOn :: !Expression
  , joinProjections :: !(NonEmpty Projection)
  } deriving (Eq, Show)

data JoinSource
  = JoinSelect Select
  | JoinReselect Reselect
  deriving (Eq, Show)

data JoinAlias = JoinAlias
  { joinAliasEntity :: Text
  } deriving (Eq, Show)

newtype Where =
  Where [Expression]
  deriving (Eq, Show)

instance Monoid Where where
  mempty = Where mempty

instance Semigroup Where where
  (Where x) <> (Where y) = Where (x <> y)

data Top
  = NoTop
  | Top Int
  deriving (Eq, Show)

instance Monoid Top where
  mempty = NoTop

instance Semigroup Top where
  (<>) :: Top -> Top -> Top
  (<>) NoTop x = x
  (<>) x NoTop = x
  (<>) (Top x) (Top y) = Top (min x y)

data Value
  = IntValue Int
  | BoolValue Bool
  | TextValue Text
  | FloatValue Float
  | DoubleValue Double
  | NullValue
  deriving (Show, Eq)

data Expression
  = ValueExpression Value
  | AndExpression [Expression]
  | OrExpression [Expression]
  | NotExpression Expression
  | ExistsExpression Select
  | SelectExpression Select
  | ReselectExpression Reselect
  | ArrayExpression Expression
  | IsNullExpression Expression
  | IsNotNullExpression Expression
  | ColumnExpression FieldName
  | EntityExpression EntityAlias
  | EqualExpression Expression Expression
  | NotEqualExpression Expression Expression
  | JsonQueryExpression Expression
    -- ^ This one acts like a "cast to JSON" and makes SQL Server
    -- behave like it knows your field is JSON and not double-encode
    -- it.
  | ToStringExpression Expression
  | JsonValueExpression Expression JsonPath
    -- ^ This is for getting actual atomic values out of a JSON
    -- string.
  | OpExpression Op Expression Expression
  deriving (Eq, Show)

data JsonPath
  = RootPath
  | FieldPath JsonPath Text
  | IndexPath JsonPath Integer
  deriving (Eq, Show)

data Aggregate
  = CountAggregate Countable
  | OpAggregate !Text (NonEmpty Expression)
  | TextAggregate !Text
  deriving (Eq, Show)

data Countable
  = StarCountable
  | NonNullFieldCountable (NonEmpty FieldName)
  | DistinctCountable (NonEmpty FieldName)
  deriving (Eq, Show)

data From
  = FromQualifiedTable (Aliased TableName)
  | FromOpenJson (Aliased OpenJson)
  deriving (Eq, Show)

data OpenJson = OpenJson
  { openJsonExpression :: Expression
  , openJsonWith :: NonEmpty JsonFieldSpec
  } deriving (Eq, Show)

data JsonFieldSpec
  = IntField Text
  | JsonField Text
  deriving (Eq, Show)

data Aliased a = Aliased
  { aliasedThing :: !a
  , aliasedAlias :: !Text
  } deriving (Eq, Show, Functor)

newtype SchemaName = SchemaName
  { schemaNameParts :: [Text]
  } deriving (Eq, Show)

data TableName = TableName
  { tableName :: Text
  , tableNameSchema :: Text
  } deriving (Eq, Show)

data FieldName = FieldName
  { fieldName :: Text
  , fieldNameEntity :: !Text
  } deriving (Eq, Show)

data Comment = DueToPermission | RequestedSingleObject
  deriving (Eq, Show)

newtype EntityAlias = EntityAlias
  { entityAliasText :: Text
  } deriving (Eq, Show)

data Op
  = LessOp
  | LessOrEqualOp
  | MoreOp
  | MoreOrEqualOp
  -- | SIN
  -- | SNE
  -- | SLIKE
  -- | SNLIKE
  -- | SILIKE
  -- | SNILIKE
  -- | SSIMILAR
  -- | SNSIMILAR
  -- | SGTE
  -- | SLTE
  -- | SNIN
  -- | SContains
  -- | SContainedIn
  -- | SHasKey
  -- | SHasKeysAny
  -- | SHasKeysAll
  deriving (Eq, Show)
