{-# LANGUAGE OverloadedStrings #-}

-- | Convert the simple T-SQL AST to an SQL query, ready to be passed
-- to the odbc package's query/exec functions.

module Hasura.SQL.Tsql.ToQuery
  ( fromSelect
  , toQuery
  , Printer
  ) where

import           Data.Foldable
import           Data.List (intersperse)
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           Data.Maybe
import           Data.String
import           Data.Text (Text)
import qualified Data.Text as T
import           Database.ODBC.SQLServer
import           Hasura.SQL.Tsql.Types
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

(<+>?) :: Printer -> Maybe Printer -> Printer
(<+>?) x Nothing = x
(<+>?) x (Just y) = SeqPrinter [x,y]

--------------------------------------------------------------------------------
-- Printer generators

fromExpression :: Expression -> Printer
fromExpression =
  \case
    JsonQueryExpression e -> "JSON_QUERY(" <+> fromExpression e <+> ")"
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
    ColumnExpression fieldName -> fromFieldName fieldName
    EqualExpression x y ->
      "(" <+> fromExpression x <+> ") = (" <+> fromExpression y <+> ")"

fromFieldName :: FieldName -> Printer
fromFieldName (FieldName {..}) =
  fromNameText fieldNameEntity <+> "." <+> fromNameText fieldName

fromSelect :: Select -> Printer
fromSelect Select {..} =
  SepByPrinter
    NewlinePrinter
    [ "SELECT " <+>
      IndentPrinter
        7
        (SepByPrinter
           (QueryPrinter "," <+> NewlinePrinter)
           (map fromProjection (toList selectProjections)))
    , "FROM " <+> IndentPrinter 5 (fromFrom selectFrom)
    , SepByPrinter
        NewlinePrinter
        (map
           (\Join {..} ->
              SeqPrinter
                [ "OUTER APPLY ("
                , IndentPrinter 13 (fromSelect joinSelect)
                , ") "
                , NewlinePrinter
                , "AS "
                , fromJoinAlias joinJoinAlias
                ])
           selectJoins)
    , fromWhere selectWhere
    , fromOrderBys selectTop selectOffset selectOrderBy
    , fromFor selectFor
    ]

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
                   (QueryPrinter "," <+> NewlinePrinter)
                   (concatMap fromOrderBy (toList orderBys))
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


fromOrderBy :: OrderBy -> [Printer]
fromOrderBy OrderBy {..} =
  [ fromNullsOrder orderByFieldName orderByNullsOrder
  , fromFieldName orderByFieldName <+> " " <+> fromOrder orderByOrder
  ]

fromOrder :: Order -> Printer
fromOrder =
  \case
    AscOrder -> "ASC"
    DescOrder -> "DESC"

fromNullsOrder :: FieldName -> NullsOrder -> Printer
fromNullsOrder fieldName =
  \case
    NullsAnyOrder -> ""
    NullsFirst -> "IIF(" <+> fromFieldName fieldName <+> " IS NULL, 0, 1)"
    NullsLast -> "IIF(" <+> fromFieldName fieldName <+> " IS NULL, 1, 0)"

fromJoinAlias :: JoinAlias -> Printer
fromJoinAlias JoinAlias {..} =
  fromNameText joinAliasEntity <+>?
  fmap (\name -> "(" <+> fromNameText name <+> ")") joinAliasField

fromFor :: For -> Printer
fromFor =
  \case
    NoFor -> ""
    JsonFor cardinality ->
      "FOR JSON PATH" <+>
      case cardinality of
        JsonArray -> ""
        JsonSingleton -> ", WITHOUT_ARRAY_WRAPPER"

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
    OpAggregate text fieldName ->
      QueryPrinter (rawUnescapedText text) <+> "(" <+> fromFieldName fieldName <+> ")"
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

fromTableName :: TableName -> Printer
fromTableName TableName {tableName, tableNameSchema} =
  fromNameText tableNameSchema <+> "." <+> fromNameText tableName

fromAliased :: Aliased Printer -> Printer
fromAliased Aliased {..} =
  aliasedThing <+>
  ((" AS " <+>) . fromNameText) aliasedAlias

fromNameText :: Text -> Printer
fromNameText t = QueryPrinter (rawUnescapedText ("[" <> t <> "]"))

trueExpression :: Expression
trueExpression = ValueExpression (BoolValue True)

falseExpression :: Expression
falseExpression = ValueExpression (BoolValue False)

--------------------------------------------------------------------------------
-- Basic printing API

toQuery :: Printer -> Query
toQuery = go 0
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
