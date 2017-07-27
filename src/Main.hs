module Main where

import qualified Reshard.Operations as Operations
import qualified Reshard.Cli as Cli

main :: IO ()
main = Cli.parseArgs >>= Operations.reshard
