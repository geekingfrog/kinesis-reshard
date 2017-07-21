{-# LANGUAGE OverloadedStrings #-}

module Main where


import qualified Debug.Trace as Debug
import Control.Monad.IO.Class (liftIO)
import qualified Data.List as List

import Data.Text (Text)
import Reshard.Types
import Reshard.Kinesis
import Reshard.Operations
import Control.Monad

main :: IO ()
main = print $ testPure 411 (makeStream 21 0 41597004636586299241267257522613247598)

-- testAWS :: Text -> Int -> IO ()
-- testAWS streamName targetNumber = tmpRun $ do
--         -- TODO just after the stream is created, the list of shard can be empty
--         -- test for that (streamStatus = CREATING) and retry if that happen
--         shards <- (List.sort . fmap awsShard . getShards) <$> describeStream streamName
--         liftIO $ putStrLn $ "shards: " ++ show shards
--         let ops = operations targetNumber shards
--         liftIO $ print ops
--         mapM_ (Reshard.Kinesis.applyOperation streamName) ops
--         liftIO $ putStrLn "all done"

testPure :: Int -> [Shard] -> Either String [Shard]
testPure targetShardNum shards = case operations targetShardNum shards of
    Left x -> Left (show x)
    Right ops -> case foldM Reshard.Operations.applyOperation shards ops of
        Left err -> Left (show err)
        Right res -> Right res
  -- where
  --   ops = operations targetShardNum shards

  -- let
  --   initialStream = makeStream 1 0 340282366920938463463374607431768211455
  --   operations1 = reshard targetShardNum initialStream
  -- in
  --   foldM Reshard.Operations.applyOperation initialStream operations1

-- reshard :: Int -> [Shard] -> [Operation]
-- reshard targetNumShard shards =
--   let
--     finalShards = makeStream targetNumShard (shardStart $ head shards) (shardEnd $ last shards)
--     tmpShards = intermediateStream targetNumShard shards
--     splitParts = assocSplits shards tmpShards
--     splitOps = concatMap (uncurry splitShards) splitParts
--     mergeParts = assocMerge tmpShards finalShards
--     mergeOps = concatMap (uncurry (flip mergeShards)) mergeParts
--   in
--     splitOps ++ mergeOps
--
--
-- makeStream :: Int -> Integer -> Integer -> [Shard]
-- makeStream shardNumber start end = shards
--   where
--     l = (end - start) `div` n
--     n = fromIntegral shardNumber
--     shards = [Shard
--         { shardStart = start + i * l + j
--         , shardEnd = end'
--         } | i <- [0.. n - 1]
--             , let j = if i == 0 then 0 else 1
--             , let end' = if i == (n - 1) then end else start + (i+1) * l
--             ]
--
-- intermediateStream :: Int -> [Shard] -> [Shard]
-- intermediateStream finalShardNumber startingShards =
--   let
--     finalShards = makeStream finalShardNumber (shardStart $ head startingShards) (shardEnd $ last startingShards)
--     intermediateShards = mergeStream startingShards finalShards
--   in
--     intermediateShards
--
--
-- -- TODO handle case where a resulting shard is impossible (start == end)
-- -- TODO find better name
-- -- Given two lists of shards, returns a new list where every every shard
-- -- starts and ends at one of the two input's shard
-- -- |    |    |
-- -- |  |   |  |
-- -- wil give
-- -- |  | | |  |
-- mergeStream :: [Shard] -> [Shard] -> [Shard]
-- mergeStream = go
--   where
--     go [] y = y
--     go x [] = x
--     go (x:xs) (y:ys) =
--       let
--         xStart = shardStart x
--         xEnd = shardEnd x
--         yStart = shardStart y
--         yEnd = shardEnd y
--         start = min xStart yStart
--         end = let e
--                     | start == xStart && start == yStart = min xEnd yEnd
--                     | start == xStart = min3 yStart xEnd yEnd
--                     | otherwise = min3 xStart xEnd yEnd
--               in e
--         newShard = Shard { shardStart = start, shardEnd = end }
--         xs' = if newShard == x
--                 then xs
--                 else Shard {shardStart = end + 1, shardEnd = xEnd} : xs
--         ys' = if newShard == y
--                 then ys
--                 else Shard {shardStart = end + 1, shardEnd = yEnd} : ys
--       in
--         newShard : go xs' ys'
--
--
-- -- TODO find a better name for that
-- -- assocSplits shardsToSplit finalShards will return a list of
-- -- (shardToSplit, newShardsAfterSplit)
-- -- precondition: length shardsToSplit <= length finalShards
-- -- AND each shard boundary in shardsToSplit has an equivalent in finalShards
-- assocSplits :: [Shard] -> [Shard] -> [(Shard, [Shard])]
-- assocSplits = assoc
--
--
-- -- assocMerge shardsToMerge finalShards will return a list of
-- -- (finalShard, shardsToMerge)
-- -- precondition: length shardsToMerge >= length finalShards
-- -- AND each shard boundary in finalShards has an equivalent in shardsToMerge
-- assocMerge :: [Shard] -> [Shard] -> [(Shard, [Shard])]
-- assocMerge shardsToMerge finalShards = assoc finalShards shardsToMerge
--
--
-- assoc :: [Shard] -> [Shard] -> [(Shard, [Shard])]
-- assoc a b = filter (not . null) $ assoc' a b
--   where
--     assoc' [] _ = []
--     assoc' (x:xs) intermediateShards =
--       let
--         (segment, rest) = List.span (\sh -> shardStart sh < shardEnd x) intermediateShards
--       in
--         (x, segment) : assoc' xs rest
--
--
-- -- List of split operations to go from a given shard to the given list of shards
-- splitShards :: Shard -> [Shard] -> [Operation]
-- splitShards seed [x] = [Noop seed]
-- splitShards seed [x, y] = [Split seed (shardEnd x)]
-- splitShards seed ends =
--   let
--     mid = toInteger $ ceiling $ fromIntegral (shardEnd seed - shardStart seed) / 2 + fromIntegral (shardStart seed)
--     (half1, half2) = List.span (\s -> shardEnd s <= mid) ends
--     realMid = shardStart (head half2) - 1
--     seed1 = Shard { shardStart = shardStart seed, shardEnd = realMid }
--     seed2 = Shard { shardStart = realMid + 1, shardEnd = shardEnd seed }
--   in
--     Split seed realMid : splitShards seed1 half1 ++ splitShards seed2 half2
--
--
-- mergeShards :: [Shard] -> Shard -> [Operation]
-- mergeShards [] _ = []
-- mergeShards [x] _ = []
-- mergeShards (x1:x2:xs) target = Merge (x1, x2) : mergeShards xs target
--
--
-- applyOperation :: [Shard] -> Operation -> [Shard]
-- applyOperation [] _ = []
-- applyOperation xs (Noop _) = xs
-- applyOperation (x:xs) (Split a n) | x == a =
--     Shard (shardStart x) n : Shard (n+1) (shardEnd x) : xs
-- applyOperation (x1:x2:xs) (Merge (a,b)) | (a == x1) && (b == x2) =
--     Shard (shardStart x1) (shardEnd x2) : xs
-- applyOperation (x:xs) op = x : applyOperation xs op
--
--
-- validStream :: Stream -> Bool
-- validStream s = and
--     [ streamStart s < streamEnd s
--     , not $ null shards
--     , streamStart s == shardStart (head shards)
--     , streamEnd s == shardEnd (last shards)
--     , all (\sh -> shardStart sh < shardEnd sh) shards
--     , all (\(sh1, sh2) -> shardEnd sh1 + 1 == shardStart sh2) (zip shards (tail shards))
--     ]
--   where
--     shards = streamShards s
--
--
-- fromShards :: [Shard] -> Stream
-- fromShards shards = Stream
--     { streamStart = shardStart (head shards)
--     , streamEnd = shardEnd (last shards)
--     , streamShards = shards
--     }
--
--
-- min3 :: Ord a => a -> a -> a -> a
-- min3 a b c = min a (min b c)
--
-- testShards0 = [ Shard {shardStart=0, shardEnd=100} ]
--
-- testShards1 =
--     [ Shard {shardStart=0, shardEnd=50}
--     , Shard {shardStart=51, shardEnd=100}
--     ]
-- testShards2 =
--     [ Shard {shardStart=0, shardEnd=33}
--     , Shard {shardStart=34, shardEnd=66}
--     , Shard {shardStart=67, shardEnd=100}
--     ]
--
-- testShards3 =
--     [ Shard {shardStart=0, shardEnd=20}
--     , Shard {shardStart=21, shardEnd=40}
--     , Shard {shardStart=41, shardEnd=60}
--     , Shard {shardStart=61, shardEnd=80}
--     , Shard {shardStart=81, shardEnd=100}
--     ]
