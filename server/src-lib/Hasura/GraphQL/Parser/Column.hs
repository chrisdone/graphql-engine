module Hasura.GraphQL.Parser.Column
  ( Postgres.PGColumnValue
  , ColumnValue
  , Interface.GColumnValue(..)
  , Postgres.column
  , Postgres.mkScalarTypeName

  , UnpreparedValue
  , Interface.GUnpreparedValue(..)

  , Interface.Opaque
  , Interface.openOpaque
  , Interface.mkParameter
  ) where

import qualified Hasura.Backends.Postgres.Column as Postgres
import qualified Hasura.GraphQL.Parser.Column.Interface as Interface
import           Hasura.SQL.Backend (BackendType(..))

type UnpreparedValue = Interface.GUnpreparedValue 'Postgres

type ColumnValue = Interface.GColumnValue 'Postgres
