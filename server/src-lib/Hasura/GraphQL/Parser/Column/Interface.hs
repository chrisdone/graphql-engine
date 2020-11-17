{-# LANGUAGE StrictData #-}

module Hasura.GraphQL.Parser.Column.Interface
  ( GColumnValue(..)
  , GUnpreparedValue(..)
  , Opaque(..)
  , openOpaque
  , mkParameter
  ) where

import           Hasura.Prelude

import           Hasura.Backends.Postgres.SQL.Types
import           Hasura.GraphQL.Parser.Class
import           Hasura.GraphQL.Parser.Schema
import           Hasura.RQL.Types.Column               hiding (EnumValue (..), EnumValueInfo (..))
import           Hasura.Session                        (SessionVariable)
import           Hasura.RQL.Types.Common

-- -------------------------------------------------------------------------------------------------

data Opaque a = Opaque
  { _opVariable :: Maybe VariableInfo
  -- ^ The variable this value came from, if any.
  , _opValue    :: a
  } -- Note: we intentionally don’t derive any instances here, since that would
    -- defeat the opaqueness!

openOpaque :: MonadParse m => Opaque a -> m a
openOpaque (Opaque Nothing  value) = pure value
openOpaque (Opaque (Just _) value) = markNotReusable $> value

data GUnpreparedValue b
  -- | A SQL value that can be parameterized over.
  = UVParameter (GColumnValue b)
                (Maybe VariableInfo)
                -- ^ The GraphQL variable this value came from, if any.
  -- | A literal SQL expression that /cannot/ be parameterized over.
  | UVLiteral (SQLExpression b)
  -- | The entire session variables JSON object.
  | UVSession
  -- | A single session variable.
  | UVSessionVar (PGType (ScalarType b)) SessionVariable

data GColumnValue b = PGColumnValue
  { pcvType  :: ColumnType b
  , pcvValue :: WithScalarType (ScalarValue b)
  }

mkParameter :: Opaque (GColumnValue b) -> GUnpreparedValue b
mkParameter (Opaque variable value) = UVParameter value variable
