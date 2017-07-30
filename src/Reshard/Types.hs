module Reshard.Types where

import Data.Typeable
import Data.Text (Text)
import Control.Exception

data Shard = Shard
    { shardStart :: !Integer
    , shardEnd :: !Integer
    } deriving (Eq)

instance Show Shard where
    show s = "Shard[" ++ show (shardStart s) ++ "-" ++ show (shardEnd s) ++ "]"

instance Ord Shard where
    compare s1 s2 = compare (shardStart s1) (shardStart s2)


data AWSShard = AWSShard
    { awsShardId :: !Text
    , awsShardParentIds :: [Text]
    , awsShard :: Shard
    } deriving (Show, Eq)

data Operation
    = Split Shard !Integer -- shard, newStartKey
    -- split a shard from (a, b) to (a, newStartKey-1) (newStartKey, b)
    | Merge (Shard, Shard)
    | Noop Shard
    deriving (Show, Eq)


newtype ReshardException = ShardNotFoundException Text
    deriving (Show, Typeable)

instance Exception ReshardException

newtype StreamNotFoundException = StreamNotFoundException Text
    deriving (Show, Typeable)

instance Exception StreamNotFoundException


data Options = Options
    { optStreamName :: !Text
    , optNumberOfShard :: !Int
    , optAWSProfile :: Maybe Text
    } deriving (Show)
