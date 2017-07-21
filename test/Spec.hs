module Spec (specs) where

import Test.Hspec
import Reshard.Types
import qualified Reshard.Operations as Op

specs :: IO ()
specs = hspec $
    describe "edge cases with too small shards" $ do
        it "preserve shards from left" $
            Op.intermediateStream 2 [Shard 0 6, Shard 7 10] `shouldBe` [Shard 0 6, Shard 7 10]
        it "preserve shards from right" $
            Op.intermediateStream 2 [Shard 0 4, Shard 5 10] `shouldBe` [Shard 0 4, Shard 5 10]
        it "preserve shards" $
            Op.intermediateStream 2 [Shard 0 5, Shard 6 10] `shouldBe` [Shard 0 5, Shard 6 10]
