{-# LANGUAGE OverloadedStrings #-}

-- | Convert the simple BigQuery AST to an SQL query, ready to be passed
-- to the odbc package's query/exec functions.

module Hasura.SQL.BigQuery.ToQuery
  ( fromSelect
  , fromReselect
  , toQueryFlat
  , toQueryPretty
  , Printer(..)
  ) where

import           Data.Foldable
import           Data.List (intersperse)
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Maybe
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Builder as LT
import           Database.ODBC.SQLServer
import           Hasura.SQL.BigQuery.Types
import           Prelude

--------------------------------------------------------------------------------
-- Types

data Printer
  = SeqPrinter [Printer]
  | SepByPrinter Printer [Printer]
  | NewlinePrinter
  | QueryPrinter Query
  | IndentPrinter Int Printer
  deriving (Show, Eq)

instance IsString Printer where
  fromString = QueryPrinter . fromString

(<+>) :: Printer -> Printer -> Printer
(<+>) x y = SeqPrinter [x,y]

--------------------------------------------------------------------------------
-- Printer generators

fromExpression :: Expression -> Printer
fromExpression =
  \case
    JsonQueryExpression e -> "JSON_QUERY(" <+> fromExpression e <+> ")"
    JsonValueExpression e path ->
      "JSON_VALUE(" <+> fromExpression e <+> fromPath path <+> ")"
    ValueExpression value -> QueryPrinter (toSql value)
    AndExpression xs ->
      SepByPrinter
        (NewlinePrinter <+> "AND ")
        (toList
           (fmap
              (\x -> "(" <+> fromExpression x <+> ")")
              (fromMaybe (pure trueExpression) (NE.nonEmpty xs))))
    OrExpression xs ->
      SepByPrinter
        (NewlinePrinter <+> " OR ")
        (toList
           (fmap
              (\x -> "(" <+> fromExpression x <+> ")")
              (fromMaybe (pure falseExpression) (NE.nonEmpty xs))))
    NotExpression expression -> "NOT " <+> (fromExpression expression)
    ExistsExpression select -> "EXISTS (" <+> fromSelect select <+> ")"
    IsNullExpression expression ->
      "(" <+> fromExpression expression <+> ") IS NULL"
    IsNotNullExpression expression ->
      "(" <+> fromExpression expression <+> ") IS NOT NULL"
    ColumnExpression fieldName -> fromFieldName fieldName
    EntityExpression fieldName -> fromEntityAlias fieldName
    EqualExpression x y ->
      "(" <+> fromExpression x <+> ") = (" <+> fromExpression y <+> ")"
    NotEqualExpression x y ->
      "(" <+> fromExpression x <+> ") != (" <+> fromExpression y <+> ")"
    ToStringExpression e -> "CONCAT(" <+> fromExpression e <+> ", '')"
    SelectExpression s -> "(" <+> IndentPrinter 1 (fromSelect s) <+> ")"
    ReselectExpression s -> "(" <+> IndentPrinter 1 (fromReselect s) <+> ")"
    ArrayExpression e -> "ARRAY(" <+> IndentPrinter 6 (fromExpression e) <+> ")"
    OpExpression op x y ->
      "(" <+>
      fromExpression x <+>
      ") " <+> fromOp op <+> " (" <+> fromExpression y <+> ")"

fromOp :: Op -> Printer
fromOp =
  \case
    LessOp -> "<"
    MoreOp -> ">"
    MoreOrEqualOp -> ">="
    LessOrEqualOp -> "<="

fromPath :: JsonPath -> Printer
fromPath path =
  ", " <+> string path
  where
    string = fromExpression .
             ValueExpression . TextValue . LT.toStrict . LT.toLazyText . go
    go =
      \case
        RootPath -> "$"
        IndexPath r i -> go r <> "[" <> LT.fromString (show i) <> "]"
        FieldPath r f -> go r <> "." <> LT.fromText f

fromFieldName :: FieldName -> Printer
fromFieldName (FieldName {..}) =
  fromNameText fieldNameEntity <+> "." <+> fromNameText fieldName

fromSelect :: Select -> Printer
fromSelect Select {..} =
  SepByPrinter
    NewlinePrinter
    [ "SELECT " <+>
      case selectAsStruct of
        AsStruct -> "AS STRUCT " <+> IndentPrinter (7 + 10) projections
        NoStruct -> IndentPrinter 7 projections
    , "FROM " <+> IndentPrinter 5 (fromFrom selectFrom)
    , SepByPrinter
        NewlinePrinter
        (map
           (\LeftOuterJoin {..} ->
              SeqPrinter
                [ "LEFT OUTER JOIN (" <+>
                  IndentPrinter 17 (fromJoinSource joinSource) <+> ")"
                , NewlinePrinter
                , "AS " <+> fromJoinAlias joinJoinAlias
                , NewlinePrinter
                , "ON (" <+> IndentPrinter 4 (fromExpression joinOn) <+> ")"
                ])
           selectJoins)
    , fromWhere selectWhere
    , fromOrderBys selectTop selectOffset selectOrderBy
    ]
  where
    projections =
      SepByPrinter
        ("," <+> NewlinePrinter)
        (map fromProjection (toList selectProjections))

fromJoinSource :: JoinSource -> Printer
fromJoinSource =
  \case
    JoinSelect select -> fromSelect select
    JoinReselect reselect -> fromReselect reselect

fromReselect :: Reselect -> Printer
fromReselect Reselect {..} =
  SepByPrinter
    NewlinePrinter
    [ "SELECT " <+>
      case reselectAsStruct of
        AsStruct -> "AS STRUCT " <+> IndentPrinter (7 + 10) projections
        NoStruct -> IndentPrinter 7 projections
    , fromWhere reselectWhere
    ]
  where
    projections =
      SepByPrinter
        ("," <+> NewlinePrinter)
        (map fromProjection (toList reselectProjections))

fromOrderBys ::
     Top -> Maybe Expression -> Maybe (NonEmpty OrderBy) -> Printer
fromOrderBys NoTop Nothing Nothing = "" -- An ORDER BY is wasteful if not needed.
fromOrderBys top moffset morderBys =
  SeqPrinter
    [ "ORDER BY "
    , IndentPrinter
        9
        (SepByPrinter
           NewlinePrinter
           [ case morderBys of
               Nothing -> "1"
               Just orderBys ->
                 SepByPrinter
                   ("," <+> NewlinePrinter)
                   (map fromOrderBy (toList orderBys))
           , case (top, moffset) of
               (NoTop, Nothing) -> ""
               (NoTop, Just offset) ->
                 "OFFSET " <+> fromExpression offset <+> " ROWS"
               (Top n, Nothing) ->
                 "OFFSET 0 ROWS FETCH NEXT " <+>
                 QueryPrinter (toSql n) <+> " ROWS ONLY"
               (Top n, Just offset) ->
                 "OFFSET " <+>
                 fromExpression offset <+>
                 " ROWS FETCH NEXT " <+> QueryPrinter (toSql n) <+> " ROWS ONLY"
           ])
    ]


fromOrderBy :: OrderBy -> Printer
fromOrderBy OrderBy {..} =
  fromFieldName orderByFieldName <+>
  " " <+>
  fromOrder orderByOrder <+> fromNullsOrder orderByFieldName orderByNullsOrder

fromOrder :: Order -> Printer
fromOrder =
  \case
    AscOrder -> "ASC"
    DescOrder -> "DESC"

fromNullsOrder :: FieldName -> NullsOrder -> Printer
fromNullsOrder _fieldName =
  \case
    NullsAnyOrder -> ""
    NullsFirst -> " NULLS FIRST"
    NullsLast -> " NULLS LAST"

fromJoinAlias :: JoinAlias -> Printer
fromJoinAlias JoinAlias {..} =
  fromNameText joinAliasEntity--  <+>?
  -- fmap (\name -> "(" <+> fromNameText name <+> ")") joinAliasField

fromProjection :: Projection -> Printer
fromProjection =
  \case
    ExpressionProjection aliasedExpression ->
      fromAliased (fmap fromExpression aliasedExpression)
    FieldNameProjection aliasedFieldName ->
      fromAliased (fmap fromFieldName aliasedFieldName)
    AggregateProjection aliasedAggregate ->
      fromAliased (fmap fromAggregate aliasedAggregate)
    StarProjection -> "*"

fromAggregate :: Aggregate -> Printer
fromAggregate =
  \case
    CountAggregate countable -> "COUNT(" <+> fromCountable countable <+> ")"
    OpAggregate text args ->
      QueryPrinter (rawUnescapedText text) <+>
      "(" <+> SepByPrinter ", " (map fromExpression (toList args)) <+> ")"
    TextAggregate text -> fromExpression (ValueExpression (TextValue text))

fromCountable :: Countable -> Printer
fromCountable =
  \case
    StarCountable -> "*"
    NonNullFieldCountable fields ->
      SepByPrinter ", " (map fromFieldName (toList fields))
    DistinctCountable fields ->
      "DISTINCT " <+>
      SepByPrinter ", " (map fromFieldName (toList fields))

fromWhere :: Where -> Printer
fromWhere =
  \case
    Where expressions ->
      case (filter ((/= trueExpression) . collapse)) expressions of
        [] -> ""
        collapsedExpressions ->
          "WHERE " <+>
          IndentPrinter 6 (fromExpression (AndExpression collapsedExpressions))
      where collapse (AndExpression [x]) = collapse x
            collapse (AndExpression []) = trueExpression
            collapse (OrExpression [x]) = collapse x
            collapse x = x

fromFrom :: From -> Printer
fromFrom =
  \case
    FromQualifiedTable aliasedQualifiedTableName ->
      fromAliased (fmap fromTableName aliasedQualifiedTableName)
    FromOpenJson openJson -> fromAliased (fmap fromOpenJson openJson)

fromOpenJson :: OpenJson -> Printer
fromOpenJson OpenJson {openJsonExpression, openJsonWith} =
  SepByPrinter
    NewlinePrinter
    [ "OPENJSON(" <+>
      IndentPrinter 9 (fromExpression openJsonExpression) <+> ")"
    , "WITH (" <+>
      IndentPrinter
        5
        (SepByPrinter
           ("," <+> NewlinePrinter)
           (toList (fmap fromJsonFieldSpec openJsonWith))) <+>
      ")"
    ]

fromJsonFieldSpec :: JsonFieldSpec -> Printer
fromJsonFieldSpec =
  \case
    IntField name -> fromNameText name <+> " INT"
    JsonField name -> fromNameText name <+> " NVARCHAR(MAX) AS JSON"

fromTableName :: TableName -> Printer
fromTableName TableName {tableName, tableNameSchema} =
  fromNameText tableNameSchema <+> "." <+> fromNameText tableName

fromAliased :: Aliased Printer -> Printer
fromAliased Aliased {..} =
  aliasedThing <+>
  ((" AS " <+>) . fromNameText) aliasedAlias

fromNameText :: Text -> Printer
fromNameText t = QueryPrinter (rawUnescapedText ("`" <> t <> "`"))

fromEntityAlias :: EntityAlias -> Printer
fromEntityAlias (EntityAlias t) = fromNameText t

trueExpression :: Expression
trueExpression = ValueExpression (BoolValue True)

falseExpression :: Expression
falseExpression = ValueExpression (BoolValue False)

--------------------------------------------------------------------------------
-- Basic printing API

toQueryFlat :: Printer -> Query
toQueryFlat = go 0
  where
    go level =
      \case
        QueryPrinter q -> q
        SeqPrinter xs -> mconcat (filter notEmpty (map (go level) xs))
        SepByPrinter x xs ->
          mconcat
            (intersperse (go level x) (filter notEmpty (map (go level) xs)))
        NewlinePrinter -> " "
        IndentPrinter n p -> go (level + n) p
    notEmpty = (/= mempty) . renderQuery

toQueryPretty :: Printer -> Query
toQueryPretty = go 0
  where
    go level =
      \case
        QueryPrinter q -> q
        SeqPrinter xs -> mconcat (filter notEmpty (map (go level) xs))
        SepByPrinter x xs ->
          mconcat
            (intersperse (go level x) (filter notEmpty (map (go level) xs)))
        NewlinePrinter -> "\n" <> indentation level
        IndentPrinter n p -> go (level + n) p
    indentation n = rawUnescapedText (T.replicate n " ")
    notEmpty = (/= mempty) . renderQuery
