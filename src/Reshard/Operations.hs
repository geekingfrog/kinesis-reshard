module Reshard.Operations where

import Data.Monoid ((<>))
import Control.Monad.IO.Class (liftIO)
import qualified Data.List as List
import Reshard.Types
import qualified Reshard.Kinesis as Kinesis

reshard :: Options -> IO ()
reshard args = Kinesis.runAWS (optAWSProfile args) $ do
    Kinesis.waitStreamActive (optStreamName args)
    description <- Kinesis.describeStream (optStreamName args)
    let shards = List.sort $ awsShard <$> Kinesis.getOpenShards description
    case operations (optNumberOfShard args) shards of
        Left _ -> liftIO $ putStrLn "ERROR!" -- TODO give more details here
        Right ops -> do
            let noops = [() | Noop _ <- ops]
            let actualNumberOfOps = length ops - length noops
            if actualNumberOfOps == 0
                then liftIO $ putStrLn "Stream already evenly split, exiting"
                else do
                    liftIO $ putStrLn $ "Found "
                        <> show (length shards)
                        <> " open shards. Target is "
                        <> show (optNumberOfShard args)
                        <> ". Will apply "
                        <> show actualNumberOfOps
                        <> " operations."
                    mapM_ (Kinesis.applyOperation (optStreamName args)) ops
                    liftIO $ putStrLn "All done."


operations :: Int -> [Shard] -> Either (Shard, [Shard]) [Operation]
operations _ [] = Right []
operations targetNumShard shards = do
    let finalShards = makeStream targetNumShard (shardStart $ head shards) (shardEnd $ last shards)
    let tmpShards = intermediateStream targetNumShard shards
    let splitParts = assocSplits shards tmpShards
    splitOps <- concat <$> sequence (fmap (uncurry splitShards) splitParts)
    let mergeParts = assocMerge tmpShards finalShards
    let mergeOps = concatMap (uncurry (flip mergeShards)) mergeParts
    pure $ splitOps ++ mergeOps


makeStream :: Int -> Integer -> Integer -> [Shard]
makeStream shardNumber start end = shards
  where
    l = (end - start) `div` n
    n = fromIntegral shardNumber
    shards = [Shard
        { shardStart = start + i * l + j
        , shardEnd = end'
        } | i <- [0.. n - 1]
            , let j = if i == 0 then 0 else 1
            , let end' = if i == (n - 1) then end else start + (i+1) * l
            ]

intermediateStream :: Int -> [Shard] -> [Shard]
intermediateStream finalShardNumber startingShards =
  let
    finalShards = makeStream finalShardNumber (shardStart $ head startingShards) (shardEnd $ last startingShards)
    intermediateShards = mergeStream startingShards finalShards
  in
    intermediateShards


-- TODO find better name
-- Given two lists of shards, returns a new list where every every shard
-- starts and ends at one of the two input's shard
-- |    |    |
-- |  |   |  |
-- wil give
-- |  | | |  |
mergeStream :: [Shard] -> [Shard] -> [Shard]
mergeStream = go
  where
    go [] y = y
    go x [] = x
    go (x:xs) (y:ys) =
      let
        xStart = shardStart x
        xEnd = shardEnd x
        yStart = shardStart y
        yEnd = shardEnd y
        newStart = max xStart yStart
        newEnd = min xEnd yEnd
        (newShard, xs', ys')
            | newEnd == xEnd && newEnd == yEnd = (Shard newStart newEnd, xs, ys)
            -- shift the final shard a bit to the left to avoid creating a shard too small
            -- The check for (null ys) should always pass
            | newEnd == xEnd && newEnd+1 >= yEnd-1 && not (null ys) =
              let
                s = Shard newStart newEnd
                ys'' = ((head ys) {shardStart = newEnd+1}) : tail ys
              in
                (s, xs, ys'')
            | newEnd == xEnd = (Shard newStart newEnd, xs, Shard (newEnd+1) yEnd : ys)
            | newEnd == yEnd && newEnd+1 >= xEnd-1 && not (null xs) =
              let s = Shard newStart xEnd
              in (s, xs, ys)
            | newEnd == yEnd = (Shard newStart newEnd, Shard (newEnd+1) xEnd : xs, ys)
            -- TODO refactor that to surface the error in the type (Either or MonadThrow)
            -- this should never happen
            | otherwise = (Shard newStart newEnd, [], [])
      in
        newShard : go xs' ys'

-- TODO find a better name for that
-- assocSplits shardsToSplit finalShards will return a list of
-- (shardToSplit, newShardsAfterSplit)
-- precondition: length shardsToSplit <= length finalShards
-- AND each shard boundary in shardsToSplit has an equivalent in finalShards
assocSplits :: [Shard] -> [Shard] -> [(Shard, [Shard])]
assocSplits = assoc


-- assocMerge shardsToMerge finalShards will return a list of
-- (finalShard, shardsToMerge)
-- precondition: length shardsToMerge >= length finalShards
-- AND each shard boundary in finalShards has an equivalent in shardsToMerge
assocMerge :: [Shard] -> [Shard] -> [(Shard, [Shard])]
assocMerge shardsToMerge finalShards = assoc finalShards shardsToMerge


assoc :: [Shard] -> [Shard] -> [(Shard, [Shard])]
assoc a b = filter (not . null) $ assoc' a b
  where
    assoc' [] _ = []
    assoc' (x:xs) intermediateShards =
      let
        (segment, rest) = List.span (\sh -> shardStart sh < shardEnd x) intermediateShards
      in
        (x, segment) : assoc' xs rest


-- List of split operations to go from a given shard to the given list of shards
splitShards :: Shard -> [Shard] -> Either (Shard, [Shard]) [Operation]
splitShards _ [] = Right []
splitShards seed [x] = if x == seed
    then Right [Noop seed]
    else Left (seed, [x, x, x])
splitShards seed (x:xs) =
  let
    splitPoint = shardEnd x + 1
    seed' = Shard splitPoint (shardEnd seed)
    splitOp = Split seed splitPoint
  in
    (splitOp :) <$> splitShards seed' xs

mergeShards :: [Shard] -> Shard -> [Operation]
mergeShards [] _ = []
mergeShards [_] _ = []
mergeShards (x1:x2:xs) target =
  let
    newShard = Shard (shardStart x1) (shardEnd x2)
  in
    Merge (x1, x2) : mergeShards (newShard : xs) target


-- simulate merge/split on aws in a pure context
applyOperation :: [Shard] -> Operation -> Either Operation [Shard]
applyOperation [] _ = Right []
applyOperation xs (Noop _) = Right xs
applyOperation (x:xs) op@(Split a n) | x == a =
    if shardStart x == n - 1 && shardEnd x > n
        then Left op
        else Right $ Shard (shardStart x) (n-1) : Shard n (shardEnd x) : xs
applyOperation (x1:x2:xs) op@(Merge (a,b)) | (a == x1) && (b == x2) =
    if shardEnd x1 + 1 /= shardStart x2
        then Left op
        else Right $ Shard (shardStart x1) (shardEnd x2) : xs
applyOperation (x:xs) op = fmap (x :) (applyOperation xs op)

validStream :: [Shard] -> Bool
validStream shards = null shards
    && all (\sh -> shardStart sh < shardEnd sh) shards
    && all (\(sh1, sh2) -> shardEnd sh1 + 1 == shardStart sh2) (zip shards (tail shards))
