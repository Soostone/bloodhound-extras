{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeFamilies      #-}
module Database.Bloodhound.Tests.Extras
    ( tests
    , TestWithESVersion
    ) where


-------------------------------------------------------------------------------
import qualified Control.Lens                        as L
import           Control.Lens.Operators              ((&), (.~))
import           Control.Monad
import qualified Control.Monad.Catch                 as CMC
import           Data.Aeson
import           Data.ByteString.Lazy                (ByteString)
import           Data.Conduit
import qualified Data.Conduit.List                   as CL
import           Data.List
import           Data.Monoid
import           Data.Tagged
import           Data.Text                           (Text)
import           Data.Typeable
import qualified Database.V1.Bloodhound              as ESV1
import qualified Database.V5.Bloodhound              as ESV5
import           Network.HTTP.Client                 (Manager, Response,
                                                      defaultManagerSettings,
                                                      newManager, responseBody,
                                                      responseStatus)
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.Options
-------------------------------------------------------------------------------
import           Database.Bloodhound.Extras.Internal
-------------------------------------------------------------------------------


tests :: TestTree
tests = askOption $ \vers -> testGroup "Database.Bloodhound.Extras"
  [ case vers of
      TestV1 -> esTests (Proxy :: Proxy ESV1)
      TestV5 -> esTests (Proxy :: Proxy ESV5)
  ]


-------------------------------------------------------------------------------
esTests
  :: ( TestESVersion v
     , FromJSON (SearchResult v Doc)
     , Eq (EsError v)
     , Show (EsError v)
     , CMC.MonadThrow (BH v IO)
     , MonadBH v (BH v IO)
     )
  => Proxy v
  -> TestTree
esTests prx = testGroup "ElasticSearch"
  [ withResource (mkBH prx) (closeBH prx) $ \newBH -> testCaseSteps "deleteWhere" $ \step -> do
      step "Creating index"
      bh <- newBH
      let runIt = runBH prx bh

      let doc1 = Doc "one"
      let doc2 = Doc "two"
      step "Index 1"
      assertSuccess =<< runIt (indexDocument prx ixn' mn' ids doc1 (mkDocId prx "1"))
      step "Index 2"
      assertSuccess =<< runIt (indexDocument prx ixn' mn' ids doc2 (mkDocId prx "2"))
      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex prx ixn')
      let q = mkTermQuery prx (mkTerm prx "name" "one")
      step "Deleting"
      res <- runBH prx bh (deleteWhereVersion prx ixn' mn' (Just q) Nothing (mkSize prx 1) 10)
      res @?= Right 1

      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex prx ixn')

      step "Searching"
      survivors <- parseEsResponse prx =<< runIt (searchByType prx ixn' mn' search')
      (map (hitSource prx) . hits prx . searchHits prx <$> survivors) @?= Right [Just doc2]
  , withResource (mkBH prx) (closeBH prx) $ \newBH -> testCaseSteps "streamingSearch'" $ \step -> do
      step "Creating index"
      bh <- newBH
      let runIt = runBH prx bh
      let doc1 = Doc "one"
      let doc2 = Doc "two"
      step "Index 1"
      assertSuccess =<< runIt (indexDocument prx ixn' mn' ids doc1 (mkDocId prx "1"))
      step "Index 2"
      assertSuccess =<< runIt (indexDocument prx ixn' mn' ids doc2 (mkDocId prx "2"))
      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex prx ixn')
      step "Searching"
      dox <- runBH prx bh (runConduit (streamingSearchVersion prx ixn' mn' search' 1 .| CL.consume))
      -- can't guarantee order
      (sort . map (hitSource prx) <$> sequence dox) @?= Right [Just doc1, Just doc2]
  ]
  where
    search' = search prx
    ixn' = ixn prx
    mn' = mn prx
    ids = defaultIndexDocumentSettings prx


-------------------------------------------------------------------------------
data TestWithESVersion = TestV1
                       | TestV5
                       deriving (Typeable)


instance IsOption TestWithESVersion where
  defaultValue = TestV1
  parseValue "1" = Just TestV1
  parseValue "5" = Just TestV5
  parseValue _   = Nothing
  optionName = Tagged "es-version"
  optionHelp = Tagged "Version of ES to test against, either 1 or 5, defaulting to 1."


class (ESVersion v) => TestESVersion v where
  type BHEnv v
  type Server v
  type IndexSettings v
  type IndexDocumentSettings v
  type ShardCount v
  type ReplicaCount v
  type Term v
  type BH v :: (* -> *) -> * -> *
  mkMappingName :: proxy v -> Text -> MappingName v
  defaultIndexSettings :: proxy v -> IndexSettings v
  defaultIndexDocumentSettings :: proxy v -> IndexDocumentSettings v
  indexShards :: proxy v -> L.Lens' (IndexSettings v) (ShardCount v)
  indexReplicas :: proxy v -> L.Lens' (IndexSettings v) (ReplicaCount v)
  mkShardCount :: proxy v -> Int -> ShardCount v
  mkReplicaCount :: proxy v -> Int -> ReplicaCount v
  mkBHEnv :: proxy v -> Server v -> Manager -> BHEnv v
  mkServer :: proxy v -> Text -> Server v
  mkSize :: proxy v -> Int -> Size v
  mkIndexName :: proxy v -> Text -> IndexName v
  mkDocId :: proxy v -> Text -> DocId v
  hitSource :: proxy v -> Hit v a -> Maybe a
  runBH :: proxy v -> BHEnv v -> BH v m a -> m a
  deleteIndex :: MonadBH v m => proxy v -> IndexName v -> m (Response ByteString)
  createIndex :: MonadBH v m => proxy v -> IndexSettings v -> IndexName v -> m (Response ByteString)
  indexDocument :: (ToJSON doc, MonadBH v m) => proxy v -> IndexName v -> MappingName v -> IndexDocumentSettings v -> doc -> DocId v -> m (Response ByteString)
  refreshIndex :: MonadBH v m => proxy v -> IndexName v -> m (Response ByteString)
  mkTerm :: proxy v -> Text -> Text -> Term v
  mkTermQuery :: proxy v -> Term v -> Query v
  searchByType :: MonadBH v m => proxy v -> IndexName v -> MappingName v -> Search v -> m (Response ByteString)


instance TestESVersion ESV1 where
  type BHEnv ESV1 = ESV1.BHEnv
  type Server ESV1 = ESV1.Server
  type IndexSettings ESV1 = ESV1.IndexSettings
  type IndexDocumentSettings ESV1 = ESV1.IndexDocumentSettings
  type ShardCount ESV1 = ESV1.ShardCount
  type ReplicaCount ESV1 = ESV1.ReplicaCount
  type Term ESV1 = ESV1.Term
  type BH ESV1 = ESV1.BH
  mkMappingName _ = ESV1.MappingName
  defaultIndexSettings _ = ESV1.defaultIndexSettings
  defaultIndexDocumentSettings _ = ESV1.defaultIndexDocumentSettings
  indexShards _ = L.lens ESV1.indexShards (\x v -> x { ESV1.indexShards = v})
  indexReplicas _ = L.lens ESV1.indexReplicas (\x v -> x { ESV1.indexReplicas = v})
  mkShardCount _ = ESV1.ShardCount
  mkReplicaCount _ = ESV1.ReplicaCount
  mkBHEnv _ = ESV1.mkBHEnv
  mkServer _ = ESV1.Server
  mkSize _ = ESV1.Size
  mkIndexName _ = ESV1.IndexName
  mkDocId _ = ESV1.DocId
  hitSource _ = ESV1.hitSource
  runBH _ = ESV1.runBH
  deleteIndex _ = ESV1.deleteIndex
  createIndex _ = ESV1.createIndex
  indexDocument _ = ESV1.indexDocument
  refreshIndex _ = ESV1.refreshIndex
  mkTerm _ = ESV1.Term
  mkTermQuery _ t = ESV1.TermQuery t Nothing
  searchByType _ = ESV1.searchByType


instance TestESVersion ESV5 where
  type BHEnv ESV5 = ESV5.BHEnv
  type Server ESV5 = ESV5.Server
  type IndexSettings ESV5 = ESV5.IndexSettings
  type IndexDocumentSettings ESV5 = ESV5.IndexDocumentSettings
  type ShardCount ESV5 = ESV5.ShardCount
  type ReplicaCount ESV5 = ESV5.ReplicaCount
  type Term ESV5 = ESV5.Term
  type BH ESV5 = ESV5.BH
  mkMappingName _ = ESV5.MappingName
  defaultIndexSettings _ = ESV5.defaultIndexSettings
  defaultIndexDocumentSettings _ = ESV5.defaultIndexDocumentSettings
  indexShards _ = L.lens ESV5.indexShards (\x v -> x { ESV5.indexShards = v})
  indexReplicas _ = L.lens ESV5.indexReplicas (\x v -> x { ESV5.indexReplicas = v})
  mkShardCount _ = ESV5.ShardCount
  mkReplicaCount _ = ESV5.ReplicaCount
  mkBHEnv _ = ESV5.mkBHEnv
  mkServer _ = ESV5.Server
  mkSize _ = ESV5.Size
  mkIndexName _ = ESV5.IndexName
  mkDocId _ = ESV5.DocId
  hitSource _ = ESV5.hitSource
  runBH _ = ESV5.runBH
  deleteIndex _ = ESV5.deleteIndex
  createIndex _ = ESV5.createIndex
  indexDocument _ = ESV5.indexDocument
  refreshIndex _ = ESV5.refreshIndex
  mkTerm _ = ESV5.Term
  mkTermQuery _ t = ESV5.TermQuery t Nothing
  searchByType _ = ESV5.searchByType


-------------------------------------------------------------------------------
search :: TestESVersion v => proxy v -> Search v
search prx = mkSearch
  prx
  Nothing
  Nothing
  Nothing
  (mkFrom prx 0)
  (mkSize prx 10)
  (searchTypeQueryThenFetch prx)


-------------------------------------------------------------------------------
assertSuccess :: Response ByteString -> Assertion
assertSuccess reply = assertBool (show stat <> ": " <> show (responseBody reply))
                                 (ESV1.isSuccess reply)
  where stat = responseStatus reply

-------------------------------------------------------------------------------
ixn :: TestESVersion v => proxy v -> IndexName v
ixn prx = mkIndexName prx "bloodhound-extras-tests"


-------------------------------------------------------------------------------
ixs :: TestESVersion v => proxy v -> IndexSettings v
ixs prx = (defaultIndexSettings prx)
  & indexShards prx .~ mkShardCount prx 1
  & indexReplicas prx .~ mkReplicaCount prx 1


-------------------------------------------------------------------------------
mn :: TestESVersion v => proxy v -> MappingName v
mn prx = mkMappingName prx "docs"


-------------------------------------------------------------------------------
newtype Doc = Doc { docName :: Text } deriving (Show, Eq, Ord)


instance ToJSON Doc where
  toJSON d = object ["name" .= docName d]


instance FromJSON Doc where
  parseJSON v = withObject "Doc" parse v
    where parse o = Doc <$> o .: "name"

-------------------------------------------------------------------------------
mkBH
  :: ( TestESVersion v
     , MonadBH v (BH v IO)
     )
  => proxy v
  -> IO (BHEnv v)
mkBH prx = do
  mgr <- newManager defaultManagerSettings
  let bh = mkBHEnv prx (testServer prx) mgr
  reply <- runBH prx bh (createIndex prx (ixs prx) (ixn prx))
  unless (ESV1.isSuccess reply) $ error (show (responseBody reply))
  return bh


-------------------------------------------------------------------------------
closeBH
  :: ( TestESVersion v
     , MonadBH v (BH v IO)
     )
  => proxy v
  -> BHEnv v
  -> IO ()
closeBH prx bhe = do
  reply <- (runBH prx bhe (deleteIndex prx (ixn prx)))
  unless (ESV1.isSuccess reply) $ error (show (responseBody reply))


-------------------------------------------------------------------------------
testServer :: TestESVersion v => proxy v -> Server v
testServer prx  = mkServer prx "http://localhost:9200"
