module Reshard.Cli where

import Options.Applicative
import Data.Monoid
import Data.Text (pack)

import Reshard.Types

options :: Parser Options
options = Options
    <$> streamArg
    <*> shardArg
    <*> profileArg
  where
    streamArg = pack <$> strArgument
        ( metavar "STREAM_NAME"
        <> help "kinesis stream name"
        )
    shardArg = argument auto
        ( metavar "NUMBER_OF_SHARD"
        <> help "desired number of shard"
        )
    profileArg = optional $ pack <$> strOption
        ( metavar "AWS_PROFILE"
        <> long "profile"
        <> short 'p'
        <> help "which aws profile to use"
        )


opts :: ParserInfo Options
opts = info
    (options <**> helper)
    (fullDesc <> progDesc "Evenly reshard a kinesis stream")

parseArgs :: IO Options
parseArgs = execParser opts

testOpts :: IO ()
testOpts = do
    o <- execParser opts
    print o
