{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeFamilies        #-}
module Database.Bloodhound.Extras.Internal where


-------------------------------------------------------------------------------
import           Control.Applicative    as A
import           Control.Monad.Catch
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Aeson
import qualified Data.ByteString.Lazy   as LBS
import qualified Data.Conduit           as C
import           Data.Conduit.Lift
import qualified Data.Conduit.List      as CL
import           Data.Monoid
import           Data.Proxy             (Proxy (..))
import           Data.Time.Clock
import qualified Data.Vector            as V
import qualified Database.V1.Bloodhound as ESV1
import qualified Database.V5.Bloodhound as ESV5
import           GHC.Exts               (Constraint)
import qualified Network.HTTP.Client    as HC
-------------------------------------------------------------------------------


class ESVersion v where
  type MonadBH v :: (* -> *) -> Constraint
  type IndexName v
  type MappingName v
  type Query v
  type Filter v
  type From v
  mkFrom :: proxy v -> Int -> From v
  type Size v
  type EsError v
  type Search v
  type Sort v
  type Aggregations v
  type SearchType v
  searchTypeQueryThenFetch :: proxy v -> SearchType v
  type FieldName v
  type Highlights v
  type SearchHits v :: * -> *
  hits :: proxy v -> SearchHits v a -> [Hit v a]
  type SearchResult v :: * -> *
  searchHits :: proxy v -> SearchResult v a -> SearchHits v a
  scrollId :: proxy v -> SearchResult v a -> Maybe (ScrollId v)
  type Source v
  mkSearch
    :: proxy v
    -> Maybe (Query v)
    -> Maybe (Filter v)
    -> Maybe (Sort v)
    -> From v
    -> Size v
    -> SearchType v
    -> Search v
  type Hit v :: * -> *
  type DocId v
  type BulkOperation v
  type ScrollId v
  bulkDelete :: proxy v -> IndexName v -> MappingName v -> DocId v -> BulkOperation v
  hitDocId :: proxy v -> Hit v a -> DocId v
  bulk :: MonadBH v m => proxy v -> V.Vector (BulkOperation v) -> m (HC.Response LBS.ByteString)
  parseEsResponse :: (MonadThrow m, FromJSON a) => proxy v -> HC.Response LBS.ByteString -> m (Either (EsError v) a)
  -- ES5 API actually returns the first page right away, ES1 just returns the scroll id
  getInitialScroll :: (MonadBH v m, MonadThrow m, FromJSON a) => proxy v -> IndexName v -> MappingName v -> Search v -> m (Maybe (Maybe (SearchResult v a), ScrollId v))
  advanceScroll :: (FromJSON a, MonadBH v m, MonadThrow m) => proxy v -> ScrollId v -> NominalDiffTime -> m (Either (EsError v) (SearchResult v a))


data ESV1

instance ESVersion ESV1 where
  type MonadBH ESV1 = ESV1.MonadBH
  type IndexName ESV1 = ESV1.IndexName
  type MappingName ESV1 = ESV1.MappingName
  type Query ESV1 = ESV1.Query
  type Filter ESV1 = ESV1.Filter
  type From ESV1 = ESV1.From
  mkFrom _ = ESV1.From
  type Size ESV1 = ESV1.Size
  type EsError ESV1 = ESV1.EsError
  type Search ESV1 = ESV1.Search
  type Sort ESV1 = ESV1.Sort
  type Aggregations ESV1 = ESV1.Aggregations
  type SearchType ESV1 = ESV1.SearchType
  searchTypeQueryThenFetch _ = ESV1.SearchTypeQueryThenFetch
  type FieldName ESV1 = ESV1.FieldName
  type Highlights ESV1 = ESV1.Highlights
  type SearchHits ESV1 = ESV1.SearchHits
  hits _ = ESV1.hits
  type SearchResult ESV1 = ESV1.SearchResult
  searchHits _ = ESV1.searchHits
  scrollId _ = ESV1.scrollId
  type Source ESV1 = ESV1.Source
  mkSearch _ query filt sort frm sz searchTy = ESV1.Search
    { queryBody = query
    , filterBody = filt
    , sortBody = sort
    , aggBody = Nothing
    , highlight = Nothing
    , trackSortScores = False
    , from = frm
    , size = sz
    , searchType = searchTy
    , fields = Nothing
    , source = Nothing
#if MIN_VERSION_bloodhound(0,15,0)
    , suggestBody = Nothing
#endif
    }
  type Hit ESV1 = ESV1.Hit
  type DocId ESV1 = ESV1.DocId
  type BulkOperation ESV1 = ESV1.BulkOperation
  type ScrollId ESV1 = ESV1.ScrollId
  bulkDelete _ = ESV1.BulkDelete
  hitDocId _ = ESV1.hitDocId
  bulk _ = ESV1.bulk
  parseEsResponse _ = ESV1.parseEsResponse
  getInitialScroll _ a b c = fmap (Nothing, ) A.<$> ESV1.getInitialScroll a b c
  advanceScroll _ = ESV1.advanceScroll


data ESV5


instance ESVersion ESV5 where
  type MonadBH ESV5 = ESV5.MonadBH
  type IndexName ESV5 = ESV5.IndexName
  type MappingName ESV5 = ESV5.MappingName
  type Query ESV5 = ESV5.Query
  type Filter ESV5 = ESV5.Filter
  type From ESV5 = ESV5.From
  mkFrom _ = ESV5.From
  type Size ESV5 = ESV5.Size
  type EsError ESV5 = ESV5.EsError
  type Search ESV5 = ESV5.Search
  type Sort ESV5 = ESV5.Sort
  type Aggregations ESV5 = ESV5.Aggregations
  type SearchType ESV5 = ESV5.SearchType
  searchTypeQueryThenFetch _ = ESV5.SearchTypeQueryThenFetch
  type FieldName ESV5 = ESV5.FieldName
  type Highlights ESV5 = ESV5.Highlights
  type SearchHits ESV5 = ESV5.SearchHits
  hits _ = ESV5.hits
  type SearchResult ESV5 = ESV5.SearchResult
  searchHits _ = ESV5.searchHits
  scrollId _ = ESV5.scrollId
  type Source ESV5 = ESV5.Source
  mkSearch _ query filt sort frm sz searchTy = ESV5.Search
    { queryBody = query
    , filterBody = filt
    , sortBody = sort
    , aggBody = Nothing
    , highlight = Nothing
    , trackSortScores = False
    , from = frm
    , size = sz
    , searchType = searchTy
    , fields = Nothing
    , source = Nothing
#if MIN_VERSION_bloodhound(0,15,0)
    , suggestBody = Nothing
#endif
    }
  type Hit ESV5 = ESV5.Hit
  type DocId ESV5 = ESV5.DocId
  type BulkOperation ESV5 = ESV5.BulkOperation
  type ScrollId ESV5 = ESV5.ScrollId
  bulkDelete _ = ESV5.BulkDelete
  hitDocId _ = ESV5.hitDocId
  bulk _ = ESV5.bulk
  parseEsResponse _ = ESV5.parseEsResponse
  getInitialScroll prx a b c = do
    res <- ESV5.getInitialScroll a b c
    pure $ case res of
      Left _ -> Nothing
      Right sr -> do
        scroll <- scrollId prx sr
        pure (Just sr, scroll)
  advanceScroll _ = ESV5.advanceScroll


-- | Delete matches for the search criteria in an index. Uses the bulk
-- interface. You are encouraged to perform a count first before
-- performing this operation. Error responses from the server will
-- terminate the process. Alias of 'deleteWhereV1'
deleteWhere
    :: ( ESV1.MonadBH m
       , MonadThrow m
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> Maybe ESV1.Query
    -> Maybe ESV1.Filter
    -> ESV1.Size
    -- ^ Per page
    -> NominalDiffTime
    -- ^ Scroll window. 60 is a reasonable default
    -> m (Either ESV1.EsError Int)
    -- ^ Number of records deleted
deleteWhere = deleteWhereV1


-------------------------------------------------------------------------------
-- | Delete matches for the search criteria in an index. Uses the bulk
-- interface. You are encouraged to perform a count first before
-- performing this operation. Error responses from the server will
-- terminate the process. For ElasticSearch Version 1.x
deleteWhereV1
    :: ( ESV1.MonadBH m
       , MonadThrow m
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> Maybe ESV1.Query
    -> Maybe ESV1.Filter
    -> ESV1.Size
    -- ^ Per page
    -> NominalDiffTime
    -- ^ Scroll window. 60 is a reasonable default
    -> m (Either ESV1.EsError Int)
    -- ^ Number of records deleted
deleteWhereV1 = deleteWhereVersion (Proxy :: Proxy ESV1)


-------------------------------------------------------------------------------
-- | Delete matches for the search criteria in an index. Uses the bulk
-- interface. You are encouraged to perform a count first before
-- performing this operation. Error responses from the server will
-- terminate the process. For ElasticSearch Version 5.x
deleteWhereV5
    :: ( ESV5.MonadBH m
       , MonadThrow m
       )
    => ESV5.IndexName
    -> ESV5.MappingName
    -> Maybe ESV5.Query
    -> Maybe ESV5.Filter
    -> ESV5.Size
    -- ^ Per page
    -> NominalDiffTime
    -- ^ Scroll window. 60 is a reasonable default
    -> m (Either ESV5.EsError Int)
    -- ^ Number of records deleted
deleteWhereV5 = deleteWhereVersion (Proxy :: Proxy ESV5)


-------------------------------------------------------------------------------
deleteWhereVersion
    :: forall proxy v m. ( ESVersion v
       , MonadBH v m
       , MonadThrow m
       )
    => proxy v
    -> IndexName v
    -> MappingName v
    -> Maybe (Query v)
    -> Maybe (Filter v)
    -> Size v
    -- ^ Per page
    -> NominalDiffTime
    -- ^ Scroll window. 60 is a reasonable default
    -> m (Either (EsError v) Int)
    -- ^ Number of records deleted
deleteWhereVersion prx ixn mn q f sz scroll = do
    res <- runExceptT (C.runConduit (stream C..| CL.foldMapM go))
    return (getSum <$> (res :: Either (EsError v) (Sum Int)))
  where search = mkSearch
                   prx
                   q
                   f
                   Nothing
                   (mkFrom prx 0)
                   sz
                   (searchTypeQueryThenFetch prx)
        stream = C.transPipe lift (streamingSearchVersion' prx ixn mn search scroll)
        go (Left e) = ExceptT (return (Left e))
        go (Right shs) = do
           let dox = V.fromList (hitDocId prx <$> ((hits prx shs) :: [Hit v Value]))
           unless (V.null dox) $ do
             _ :: Value <- ExceptT $ parseEsResponse prx =<< bulk prx (bulkDelete prx ixn mn <$> dox)
             return ()
           return ((Sum (V.length dox)))


-------------------------------------------------------------------------------
-- | Alias of 'streamingSearchV1'
streamingSearch
    :: ( ESV1.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> ESV1.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV1.EsError (ESV1.Hit a)) m ()
streamingSearch = streamingSearchV1


-------------------------------------------------------------------------------
streamingSearchV1
    :: ( ESV1.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> ESV1.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV1.EsError (ESV1.Hit a)) m ()
streamingSearchV1 = streamingSearchVersion (Proxy :: Proxy ESV1)


-------------------------------------------------------------------------------
streamingSearchV5
    :: ( ESV5.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV5.IndexName
    -> ESV5.MappingName
    -> ESV5.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV5.EsError (ESV5.Hit a)) m ()
streamingSearchV5 = streamingSearchVersion (Proxy :: Proxy ESV5)


-------------------------------------------------------------------------------
streamingSearchVersion
    :: ( ESVersion v
       , MonadBH v m
       , MonadThrow m
       , FromJSON a
       )
    => proxy v
    -> IndexName v
    -> MappingName v
    -> Search v
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either (EsError v) (Hit v a)) m ()
streamingSearchVersion prx ixn mn s scroll = streamingSearchVersion' prx ixn mn s scroll C..| C.awaitForever go
  where go (Left e)   = C.yield (Left e)
        go (Right sh) = mapM_ (C.yield . Right) (hits prx sh)


-------------------------------------------------------------------------------
streamingSearch'
    :: forall i m a. ( ESV1.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> ESV1.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV1.EsError (ESV1.SearchHits a)) m ()
streamingSearch' = streamingSearchV1'


-------------------------------------------------------------------------------
streamingSearchV1'
    :: forall i m a. ( ESV1.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV1.IndexName
    -> ESV1.MappingName
    -> ESV1.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV1.EsError (ESV1.SearchHits a)) m ()
streamingSearchV1' = streamingSearchVersion' (Proxy :: Proxy ESV1)


-------------------------------------------------------------------------------
streamingSearchV5'
    :: forall i m a. ( ESV5.MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => ESV5.IndexName
    -> ESV5.MappingName
    -> ESV5.Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either ESV5.EsError (ESV5.SearchHits a)) m ()
streamingSearchV5' = streamingSearchVersion' (Proxy :: Proxy ESV5)


-------------------------------------------------------------------------------
-- | Creates a conduit 'Producer' of search hits for the given search.
streamingSearchVersion'
    :: forall i m a v proxy. ( ESVersion v
       , MonadBH v m
       , MonadThrow m
       , FromJSON a
       )
    => proxy v
    -> IndexName v
    -> MappingName v
    -> Search v
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> C.ConduitM i (Either (EsError v) (SearchHits v a)) m ()
streamingSearchVersion' prx ixn mn s scroll = evalStateC Nothing go
  where go = do msid <- get
                case msid of
                  Nothing -> do
                    res <- lift (lift (getInitialScroll prx ixn mn s))
                    case res of
                      Just (mfirstPage, isid) -> do
                        put (Just isid)
                        case mfirstPage of
                          Just firstPage -> C.yield (Right (searchHits prx firstPage))
                          Nothing -> return ()
                        go
                      Nothing -> return ()
                  Just sid -> do
                    esr :: Either (EsError v) (SearchResult v a) <- lift (lift (advanceScroll prx sid scroll))
                    case esr of
                      Right sr -> do
                        let srHits = searchHits prx sr
                        maybe (return ()) (put . Just) (scrollId prx sr)
                        unless (null (hits prx srHits)) $ do
                          C.yield (Right srHits)
                          go
                      Left _ -> return ()


-------------------------------------------------------------------------------
hush :: Either e a -> Maybe a
hush (Right a) = Just a
hush (Left _)  = Nothing
