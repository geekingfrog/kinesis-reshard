module Reshard.Cli (parseArgs) where

import Options.Applicative
import Data.Monoid
import Data.Text (pack)

import Reshard.Types
import qualified Paths_kinesis_reshard as Meta
import Data.Version (showVersion)

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

testVersion :: Parser (a -> a)
testVersion = abortOption (InfoMsg $ "version: " <> showVersion Meta.version) $ mconcat
    [ long "version"
    , short 'V'
    , help "Show version and exit"
    ]

opts :: ParserInfo Options
opts = info
    (options <**> (testVersion <*> helper))
    (fullDesc <> progDesc "Evenly reshard a kinesis stream")

parseArgs :: IO Options
parseArgs = execParser opts
