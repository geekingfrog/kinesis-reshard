# Reshard any kinesis stream

## Usage:

```bash
$ reshard stream-name 10

# with a specific aws profile
$ reshard --profile my-profile stream-name 10
```

## Why ?

AWS has an API call to reshard a stream, but it has the following (severe) limitations:
* 2 calls *per day*
* Can only reshard within a factor of 2. So to go from 1 shard to 10, you need 4 api calls (which takes 2 days).

This tools doesn't suffer from these limitations. You may have some trouble if you try to reshard a stream close to the kinesis limit (500 shard) or your AWS account shard limit because more shards may be created during the process.


# Build and installation
You'll need the haskell build tool [stack](https://docs.haskellstack.org/en/stable/GUIDE/#downloading-and-installation). Then, simply run `stack install`.
