{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.Bloodhound.Extras
    ( deleteWhere
    , streamingSearch
    , streamingSearch'
    ) where


-------------------------------------------------------------------------------
import           Control.Monad.Catch
import           Control.Monad.Except
import           Control.Monad.State
import           Data.Aeson
import           Data.Conduit
import           Data.Conduit.Lift
import qualified Data.Conduit.List    as CL
import           Data.Monoid
import           Data.Time.Clock
import qualified Data.Vector          as V
import           Database.Bloodhound
-------------------------------------------------------------------------------


-- | Delete matches for the search criteria in an index. Uses the bulk
-- interface. You are encouraged to perform a count first before
-- performing this operation. Error responses from the server will
-- terminate the process.
deleteWhere
    :: ( MonadBH m
       , MonadThrow m
       )
    => IndexName
    -> MappingName
    -> Maybe Query
    -> Maybe Filter
    -> Size
    -- ^ Per page
    -> NominalDiffTime
    -- ^ Scroll window. 60 is a reasonable default
    -> m (Either EsError Int)
    -- ^ Number of records deleted
deleteWhere ixn mn q f sz scroll = do
    res <- runExceptT (stream $$ CL.foldMapM go)
    return (getSum <$> (res :: Either EsError (Sum Int)))
  where search = Search { queryBody = q
                        , filterBody = f
                        , sortBody = Nothing
                        , aggBody = Nothing
                        , highlight = Nothing
                        , trackSortScores = False
                        , from = From 0
                        , size = sz
                        , searchType = SearchTypeScan -- disables sorting for efficient scrolling
                        , fields = Nothing
                        , source = Nothing}
        stream = transPipe lift (streamingSearch' ixn mn search scroll)
        go (Left e) = ExceptT (return (Left e))
        go (Right shs) = do
           let dox = V.fromList (hitDocId <$> (hits shs :: [Hit Value]))
           unless (V.null dox) $ do
             _ :: Value <- ExceptT $ parseEsResponse =<< bulk (BulkDelete ixn mn <$> dox)
             return ()
           return ((Sum (V.length dox)))


-------------------------------------------------------------------------------
streamingSearch
    :: ( MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => IndexName
    -> MappingName
    -> Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> Producer m (Either EsError (Hit a))
streamingSearch ixn mn s scroll = streamingSearch' ixn mn s scroll =$ awaitForever go
  where go (Left e) = yield (Left e)
        go (Right sh) = mapM_ (yield . Right) (hits sh)


-------------------------------------------------------------------------------
-- | Creates a conduit 'Producer' of search hits for the given search.
streamingSearch'
    :: forall m a. ( MonadBH m
       , MonadThrow m
       , FromJSON a
       )
    => IndexName
    -> MappingName
    -> Search
    -> NominalDiffTime
    -- ^ How long should the scroll be open? 60s is a reasonable default
    -> Producer m (Either EsError (SearchHits a))
streamingSearch' ixn mn s scroll = evalStateC Nothing go
  where go = do msid <- get
                case msid of
                  Nothing -> do
                    misid <- lift (lift (getInitialScroll ixn mn s))
                    case misid of
                      Just _ -> put misid >> go
                      Nothing -> return ()
                  Just sid -> do
                    esr :: Either EsError (SearchResult a) <- lift (lift (advanceScroll sid scroll))
                    case esr of
                      Right sr -> do
                        let srHits = searchHits sr
                        maybe (return ()) (put . Just) (scrollId sr)
                        unless (null (hits srHits)) $ do
                          yield (Right srHits)
                          go
                      Left _ -> return ()
