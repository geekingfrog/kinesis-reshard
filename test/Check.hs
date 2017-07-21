{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Check (check) where

import Test.QuickCheck
import Reshard.Types
import qualified Reshard.Operations as Op
import Spec

newtype ShardNumber = ShardNumber Int deriving Show

instance Arbitrary ShardNumber where
    -- the real limit is 500, but 50 is large enough to uncover bug
    -- and run much more quickly
    arbitrary = ShardNumber <$> choose (1, 50)

newtype ShardRange = ShardRange Integer deriving Show

instance Arbitrary ShardRange where
    -- the real limit is *much* larger, but this should be enough for
    -- the tests and makes it easier to debug failing test cases
    arbitrary = ShardRange <$> choose (0, 1000000)
    -- arbitrary = ShardRange <$> choose (0, 340282366920938463463374607431768211455)

instance Arbitrary Shard where
    arbitrary = do
        (ShardRange start) <- arbitrary
        (ShardRange end) <- suchThat arbitrary (\(ShardRange x) -> x > start)
        pure $ Shard start end


prop_validStream :: [Shard] -> Property
prop_validStream shards = conjoin (fmap prop_validShard shards)
    .&&. conjoin (fmap (\(sh1, sh2) -> shardEnd sh1 + 1 === shardStart sh2) (zip shards (tail shards)))

prop_validShard :: Shard -> Property
prop_validShard shard = counterexample "start shard not before end shard" $ shardStart shard < shardEnd shard

prop_validMakeStream :: ShardNumber -> ShardRange -> Property
prop_validMakeStream (ShardNumber numberOfShards) (ShardRange end) =
    end > fromIntegral numberOfShards * 2 ==>
  let
    -- a stream always starts at 0
    shards = Op.makeStream numberOfShards 0 end
  in
    prop_validStream shards .&&. length shards === numberOfShards

prop_validOperation :: Operation -> Property
prop_validOperation (Noop _) = property True
prop_validOperation op@(Split shard n) =
    counterexample ("invalid split operation " ++ show op)
    $ shardStart shard < n .&&. shardEnd shard > n
prop_validOperation op@(Merge (s1, s2)) =
    counterexample ("invalid merge operation " ++ show op)
    $ shardEnd s1 === shardStart s2 - 1


operationInStream :: [Shard] -> Operation -> Property
operationInStream shards op@(Noop s) = counterexample ("operation not in stream: " ++ show op) $ s `elem` shards
operationInStream shards op@(Split s _) = counterexample ("operation not in stream: " ++ show op) $ s `elem` shards
operationInStream shards op@(Merge (s1, s2)) = counterexample ("operation not in stream: " ++ show op) $ s1 `elem` shards .&&. s2 `elem` shards


prop_validIntermediateStreams :: [Shard] -> [Operation] -> Property
prop_validIntermediateStreams _ [] = property True
prop_validIntermediateStreams shards (op:ops) =
    operationInStream shards op
    .&&.  case Op.applyOperation shards op of
        Left _ -> counterexample ("Operation failed " ++ show op) False
        Right shards' -> prop_validStream shards'
            .&&. prop_validIntermediateStreams shards' ops

prop_validOperations :: ShardNumber -> ShardNumber -> ShardRange -> Property
prop_validOperations (ShardNumber n) (ShardNumber n') (ShardRange end) =
    end > fromIntegral n * 2 && end > fromIntegral n' * 2 ==>
  let shards = Op.makeStream n 0 end
  in case Op.operations n' shards of
        Left (s, shards) -> counterexample ("Cannot perform operation on " ++ show s ++ " - " ++ show shards) False
        Right ops ->
            conjoin (fmap prop_validOperation ops)
            .&&. prop_validIntermediateStreams shards ops


check :: IO ()
check = quickCheck $ prop_validMakeStream .&&. prop_validOperations
