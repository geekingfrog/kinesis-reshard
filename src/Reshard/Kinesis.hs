{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module Reshard.Kinesis
    ( runAWS
    , describeStream
    , waitStreamActive
    , getShards
    , getOpenShards
    , applyOperation
    )
where

import Data.Maybe
import Data.Monoid
import Control.Concurrent (threadDelay)
import Control.Exception
import Data.Text hiding (concatMap)
import qualified Data.Text.IO as T
import Control.Monad.IO.Class (liftIO)
import Control.Lens
import qualified Network.AWS as AWS
import qualified Network.AWS.Data.Text as AWS
import qualified Network.AWS.Auth as AWS
import Network.AWS (AWS)
import Network.AWS.Data.Text (fromText)
import qualified Network.AWS.Kinesis as Kinesis
import qualified System.IO as IO
import Reshard.Types
import qualified Data.List as List
import qualified Data.HashSet as Set

import Control.Applicative
import Control.Monad.Trans.Maybe
import Control.Retry
import qualified System.Environment as Env
import System.Directory (getHomeDirectory)
import System.FilePath
import Conduit

import qualified Data.Ini as Ini

import Control.Monad.Reader as R

runAWS :: Maybe Text -> AWS a -> IO a
runAWS mbProfile action = do
    lgr <- AWS.newLogger AWS.Info IO.stdout
    cf <- AWS.credFile
    let cred = maybe AWS.Discover (flip AWS.FromFile cf) mbProfile
    env <- AWS.newEnv cred
    region <- fmap (fromMaybe AWS.NorthVirginia) (getRegion mbProfile)
    AWS.runResourceT $ AWS.runAWS (env & AWS.envLogger .~ lgr & AWS.envRegion .~ region) action

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
    let profile = fromMaybe "default" (("profile " <>) <$> mbProfile)
    pure $ Ini.parseIni content >>= Ini.lookupValue profile "region" >>= fromText


getEnvVariable :: String -> IO (Either String String)
getEnvVariable name = do
    result <- Control.Exception.try $ Env.getEnv name
    pure $ case result of
        Left (exc :: SomeException) -> Left (show exc)
        Right r -> Right r


hush :: Either e a -> Maybe a
hush (Left _) = Nothing
hush (Right a) = Just a


describeStream :: Text -> AWS Kinesis.DescribeStreamResponse
describeStream streamName = do
    stuff <- AWS.trying Kinesis._ResourceNotFoundException $ AWS.send $ Kinesis.describeStream streamName
    case stuff of
        Left err -> do
            env <- R.ask
            let baseMsg = maybe (streamName <> " not found") AWS.toText (err ^. AWS.serviceMessage)
            let region = AWS.toText $ env ^. AWS.envRegion
            let helpMsg = "Check the stream name. The region can be passed as environment variable AWS_REGION"
            let errMsg = baseMsg <> " (region " <> region <> ") -- " <> helpMsg
            throw $ StreamNotFoundException errMsg
        Right resp -> pure resp

getShards :: Text -> AWS [Kinesis.Shard]
getShards streamName = runConduit $ AWS.paginate (Kinesis.describeStream streamName)
    .| concatMapC (^. Kinesis.dsrsStreamDescription . Kinesis.sdShards)
    .| sinkList


-- Describe stream returns all shards, including the one already splitted/merged
-- This function only returns leaves
-- TODO handle the case when there are more shards available (hasMoreShards)
getOpenShards :: [Kinesis.Shard] -> [AWSShard]
getOpenShards kinesisShards =
  let
    allShards = fmap makeShard kinesisShards
    allParentIds = Set.fromList $ concatMap awsShardParentIds allShards
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

reshardRetry :: AWS a -> AWS a
reshardRetry action = do
    let h = logRetries (const $ pure True) (\b (e :: SomeException) r -> liftIO $ putStrLn $ defaultLogMsg b e r)
    let reshardRetryPolicy = constantDelay (500 * 1000) <> limitRetries 5
    recovering reshardRetryPolicy [h] $ const action

applyOperation :: Text -> Operation -> AWS ()
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
            waitStreamActive streamName
            reshardRetry $ AWS.send req
            reshardRetry $ waitStreamActive streamName
applyOperation streamName (Merge (shard1, shard2)) = do
    mbId1 <- findShardId streamName shard1
    mbId2 <- findShardId streamName shard2
    case (mbId1, mbId2) of
        (Just id1, Just id2) -> do
            -- TODO log instead of simple putStrLn
            liftIO $ putStrLn $ "Merging shards: " <> unpack id1 <> " and " <> unpack id2
            reshardRetry $ AWS.send $ Kinesis.mergeShards streamName id1 id2
            reshardRetry $ waitStreamActive streamName
        (Nothing, _) -> liftIO $ throwIO $ ShardNotFoundException (pack $ show shard1)
        (_, Nothing) -> liftIO $ throwIO $ ShardNotFoundException (pack $ show shard2)


findShardId :: Text -> Shard -> AWS (Maybe Text)
findShardId streamName shard = do
    shards <- getOpenShards <$> getShards streamName
    pure $ case List.find ((== shard) . awsShard) shards of
        Nothing -> Nothing
        Just (AWSShard shardId _ _) -> Just shardId

waitStreamActive :: Text -> AWS ()
waitStreamActive streamName = loop
  where
    loop = do
        resp <- describeStream streamName
        let status = resp ^. Kinesis.dsrsStreamDescription . Kinesis.sdStreamStatus
        if status /= Kinesis.Active
            then liftIO (threadDelay (2 * 10 ^ 6)) >> loop
            else pure ()
