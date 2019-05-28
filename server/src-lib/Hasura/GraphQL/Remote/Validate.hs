{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE ApplicativeDo #-}

-- | Validate input queries against remote schemas.

module Hasura.GraphQL.Remote.Validate
  ( getCreateRemoteRelationshipValidation
  , validateRelationship
  , validateRemoteArguments
  , ValidationError(..)
  ) where

import qualified Data.HashMap.Strict as HM
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Validation
import           Debug.Trace
import qualified Hasura.GraphQL.Context as GC
import           Hasura.GraphQL.Remote.Input
import           Hasura.GraphQL.Schema
import qualified Hasura.GraphQL.Schema as GS
import           Hasura.GraphQL.Validate.Types
import qualified Hasura.GraphQL.Validate.Types as VT
import           Hasura.Prelude
import           Hasura.RQL.DDL.Relationship.Types
import           Hasura.RQL.Types
import qualified Language.GraphQL.Draft.Syntax as G

-- | An error validating the remote relationship.
data ValidationError
  = CouldntFindRemoteField G.Name VT.ObjTyInfo
  | CouldntFindNamespace G.Name
  | CouldntFindTypeForNamespace G.Name
  | InvalidTypeForNamespace G.Name VT.TypeInfo
  | FieldNotFoundInRemoteSchema G.Name
  deriving (Show, Eq)

-- perhaps this old method is not the right way to find relationships now?

-- | Get a validation for the remote relationship proposal.
getCreateRemoteRelationshipValidation ::
     (QErrM m, CacheRM m)
  => CreateRemoteRelationship
  -> m (Either ValidationError ())
getCreateRemoteRelationshipValidation createRemoteRelationship = do
  schemaCache <- askSchemaCache
  (pure
     (validateRelationship
        createRemoteRelationship
        (scDefaultRemoteGCtx schemaCache)))


-- | Validate a remote relationship given a context.
validateRelationship ::
     CreateRemoteRelationship
  -> GC.GCtx
  -> Either ValidationError ()
validateRelationship createRemoteRelationship gctx = do
  objFldInfo <-
    lookupField
      (createRemoteRelationshipRemoteField createRemoteRelationship)
      (trace ("(GS._gQueryRoot gctx)=" ++ show ((GS._gQueryRoot gctx))) (GS._gQueryRoot gctx))
  case VT._fiLoc objFldInfo of
    HasuraType ->
      Left
        (FieldNotFoundInRemoteSchema
           (createRemoteRelationshipRemoteField createRemoteRelationship))
    RemoteType {} -> pure ()

-- | Lookup the field in the schema.
lookupField ::
     G.Name
  -> VT.ObjTyInfo
  -> Either ValidationError VT.ObjFldInfo
lookupField name objFldInfo = viaObject objFldInfo
  where
    viaObject =
      maybe (Left (CouldntFindRemoteField name objFldInfo)) pure .
      HM.lookup name .
      VT._otiFields

-- | Validate remote input arguments against the remote schema.
validateRemoteArguments ::
     RemoteGCtx -> RemoteArguments -> Validation (NonEmpty ValidationError) ()
validateRemoteArguments = undefined
