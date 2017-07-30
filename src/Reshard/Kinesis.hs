{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Reshard.Kinesis
    ( runAWS
    , describeStream
    , getOpenShards
    , applyOperation
    )
where

import Data.Maybe
import Data.Monoid
import Control.Concurrent (threadDelay)
import Control.Exception
import Data.Text
import qualified Data.Text.IO as T
import Control.Monad.IO.Class (liftIO)
import Control.Lens
import qualified Network.AWS as AWS
import qualified Network.AWS.Auth as AWS
import Network.AWS (MonadAWS, AWS)
import Network.AWS.Data.Text (fromText)
import qualified Network.AWS.Kinesis as Kinesis
import qualified System.IO as IO
import Reshard.Types
import qualified Data.List as List
import qualified Data.HashSet as Set

import Control.Applicative
import Control.Monad.Trans.Maybe
import qualified System.Environment as Env
import System.Directory (getHomeDirectory)
import System.FilePath

import qualified Data.Ini as Ini

runAWS :: Maybe Text -> AWS a -> IO a
runAWS mbProfile action = do
    lgr <- AWS.newLogger AWS.Info IO.stdout
    cf <- AWS.credFile
    let cred = maybe AWS.Discover (flip AWS.FromFile cf) mbProfile
    env <- AWS.newEnv cred
    region <- getRegion mbProfile
    let env' = case region of
            Nothing -> env
            Just r -> env & AWS.envRegion .~ r
    AWS.runResourceT $ AWS.runAWS (env' & AWS.envLogger .~ lgr) action

-- amazonka version 1.5.0 will be smarter about regions, but in the meantime
-- try to get the region from env variable and config file
getRegion :: Maybe Text -> IO (Maybe AWS.Region)
getRegion mbProfile = runMaybeT $ MaybeT getRegionFromEnvVariable
    <|> MaybeT (getRegionFromFile mbProfile)

getRegionFromEnvVariable :: IO (Maybe AWS.Region)
getRegionFromEnvVariable = do
    rawRegion <- getEnvVariable "AWS_REGION"
    pure $ hush (rawRegion >>= fromText . pack)

getRegionFromFile :: Maybe Text -> IO (Maybe AWS.Region)
getRegionFromFile mbProfile = fmap hush $ do
    home <- getHomeDirectory
    let configPath = home </> ".aws" </> "config"
    content <- T.readFile configPath
    let profile = fromMaybe "default" mbProfile
    pure $ Ini.parseIni content >>= Ini.lookupValue profile "region" >>= fromText


getEnvVariable :: String -> IO (Either String String)
getEnvVariable name = do
    result <- try $ Env.getEnv name
    pure $ case result of
        Left (exc :: SomeException) -> Left (show exc)
        Right r -> Right r


hush :: Either e a -> Maybe a
hush (Left _) = Nothing
hush (Right a) = Just a

describeStream :: (MonadAWS m) => Text -> m Kinesis.DescribeStreamResponse
describeStream streamName = do
    resp <- AWS.send $ Kinesis.describeStream streamName
    pure resp


-- Describe stream returns all shards, including the one already splitted/merged
-- This function only returns leaves
-- TODO handle the case when there are more shards available (hasMoreShards)
getOpenShards :: Kinesis.DescribeStreamResponse -> [AWSShard]
getOpenShards resp =
  let
    allShards = fmap makeShard (resp ^. Kinesis.dsrsStreamDescription . Kinesis.sdShards)
    allParentIds = Set.fromList $ List.concatMap awsShardParentIds allShards
  in
    List.filter (\(AWSShard sId _ _) -> not $ Set.member sId allParentIds) allShards


makeShard :: Kinesis.Shard -> AWSShard
makeShard kshard =
  let
    range = kshard ^. Kinesis.sHashKeyRange
    shard = Shard
        (read $ unpack $ range ^. Kinesis.hkrStartingHashKey)
        (read $ unpack $ range ^. Kinesis.hkrEndingHashKey)
    shardId = kshard ^. Kinesis.sShardId
    parentIds = catMaybes [kshard ^. Kinesis.sParentShardId, kshard ^. Kinesis.sAdjacentParentShardId]
  in
    AWSShard shardId parentIds shard


applyOperation :: (MonadAWS m) => Text -> Operation -> m ()
applyOperation _ (Noop _) = pure ()
applyOperation streamName (Split shard exclusiveStartKey) = do
    shardId <- findShardId streamName shard
    case shardId of
        Nothing -> liftIO $ throwIO $ ShardNotFoundException (pack $ show shard)
        Just sId -> do
            -- TODO log instead of simple putStrLn
            liftIO $ putStrLn $ "Splitting " <> unpack sId <> " at point " <> show exclusiveStartKey
            let req = Kinesis.splitShard streamName sId (pack $ show exclusiveStartKey)
            AWS.send req
            waitStreamUpdated streamName
applyOperation streamName (Merge (shard1, shard2)) = do
    mbId1 <- findShardId streamName shard1
    mbId2 <- findShardId streamName shard2
    case (mbId1, mbId2) of
        (Just id1, Just id2) -> do
            -- TODO log instead of simple putStrLn
            liftIO $ putStrLn $ "Merging shards: " <> unpack id1 <> " and " <> unpack id2
            AWS.send $ Kinesis.mergeShards streamName id1 id2
            waitStreamUpdated streamName
        (Nothing, _) -> liftIO $ throwIO $ ShardNotFoundException (pack $ show shard1)
        (_, Nothing) -> liftIO $ throwIO $ ShardNotFoundException (pack $ show shard2)


findShardId :: (MonadAWS m) => Text -> Shard -> m (Maybe Text)
findShardId streamName shard = do
    shards <- getOpenShards <$> describeStream streamName
    pure $ case List.find ((== shard) . awsShard) shards of
        Nothing -> Nothing
        Just (AWSShard shardId _ _) -> Just shardId

waitStreamUpdated :: (MonadAWS m) => Text -> m ()
waitStreamUpdated streamName = loop
  where
    loop = do
        resp <- AWS.send $ Kinesis.describeStream streamName
        let status = resp ^. Kinesis.dsrsStreamDescription . Kinesis.sdStreamStatus
        if status /= Kinesis.Active
            then liftIO (threadDelay (10 ^ 6)) >> loop
            else pure ()
