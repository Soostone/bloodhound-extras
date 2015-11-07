{-# LANGUAGE OverloadedStrings #-}
module Database.Bloodhound.Tests.Extras
    ( tests
    ) where


-------------------------------------------------------------------------------
import           Control.Monad
import           Data.Aeson
import           Data.Conduit
import qualified Data.Conduit.List          as CL
import           Data.List
import           Data.Monoid
import           Data.Text                  (Text)
import           Database.Bloodhound
import           Network.HTTP.Client
import           Test.Tasty
import           Test.Tasty.HUnit
-------------------------------------------------------------------------------
import           Database.Bloodhound.Extras
-------------------------------------------------------------------------------


tests :: TestTree
tests = testGroup "Database.Bloodhound.Extras"
  [
    withResource mkBH closeBH $ \newBH -> testCaseSteps "deleteWhere" $ \step -> do
      step "Creating index"
      bh <- newBH
      let runIt = runBH bh

      let doc1 = Doc "one"
      let doc2 = Doc "two"
      step "Index 1"
      assertSuccess =<< runIt (indexDocument ixn mn defaultIndexDocumentSettings doc1 (DocId "1"))
      step "Index 2"
      assertSuccess =<< runIt (indexDocument ixn mn defaultIndexDocumentSettings doc2 (DocId "2"))
      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex ixn)
      let q = TermQuery (Term "name" "one") Nothing
      step "Deleting"
      res <- runIt (deleteWhere ixn mn (Just q) Nothing (Size 1) 10)
      res @?= Right 1

      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex ixn)

      step "Searching"
      survivors <- parseEsResponse =<< runIt (searchByType ixn mn search)
      (map hitSource . hits . searchHits <$> survivors) @?= Right [Just doc2]
  , withResource mkBH closeBH $ \newBH -> testCaseSteps "streamingSearch'" $ \step -> do
      step "Creating index"
      bh <- newBH
      let runIt = runBH bh
      let doc1 = Doc "one"
      let doc2 = Doc "two"
      step "Index 1"
      assertSuccess =<< runIt (indexDocument ixn mn defaultIndexDocumentSettings doc1 (DocId "1"))
      step "Index 2"
      assertSuccess =<< runIt (indexDocument ixn mn defaultIndexDocumentSettings doc2 (DocId "2"))
      step "Refreshing"
      assertSuccess =<< runIt (refreshIndex ixn)
      step "Searching"
      dox <- runIt (streamingSearch ixn mn search 1 $$ CL.consume)
      -- can't guarantee order
      (sort . map hitSource <$> sequence dox) @?= Right [Just doc1, Just doc2]
  ]


-------------------------------------------------------------------------------
search :: Search
search = Search { queryBody = Nothing
                , filterBody = Nothing
                , sortBody = Nothing
                , aggBody = Nothing
                , highlight = Nothing
                , trackSortScores = False
                , from = From 0
                , size = Size 10
                , searchType = SearchTypeQueryThenFetch
                , fields = Nothing
                , source = Nothing}

-------------------------------------------------------------------------------
assertSuccess :: Reply -> Assertion
assertSuccess reply = assertBool (show stat <> ": " <> show (responseBody reply))
                                 (isSuccess reply)
  where stat = responseStatus reply

-------------------------------------------------------------------------------
ixn :: IndexName
ixn = IndexName "bloodhound-extras-tests"


-------------------------------------------------------------------------------
ixs :: IndexSettings
ixs = defaultIndexSettings { indexShards = ShardCount 1
                           , indexReplicas = ReplicaCount 1
                           }

-------------------------------------------------------------------------------
mn :: MappingName
mn = MappingName "docs"

-------------------------------------------------------------------------------
newtype Doc = Doc { docName :: Text } deriving (Show, Eq, Ord)


instance ToJSON Doc where
  toJSON d = object ["name" .= docName d]


instance FromJSON Doc where
  parseJSON v = withObject "Doc" parse v
    where parse o = Doc <$> o .: "name"

-------------------------------------------------------------------------------
mkBH :: IO BHEnv
mkBH = do
  mgr <- newManager defaultManagerSettings
  let bh = (BHEnv testServer mgr)
  reply <- runBH bh (createIndex ixs ixn)
  unless (isSuccess reply) $ error (show (responseBody reply))
  return bh


-------------------------------------------------------------------------------
closeBH :: BHEnv -> IO ()
closeBH bhe = do
  reply <- (runBH bhe (deleteIndex ixn))
  unless (isSuccess reply) $ error (show (responseBody reply))


-------------------------------------------------------------------------------
testServer  :: Server
testServer  = Server "http://localhost:9200"
