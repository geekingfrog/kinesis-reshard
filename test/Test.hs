import Spec
import Check


main :: IO ()
-- main = quickCheck $ prop_validMakeStream .&&. (withMaxSuccess 5 prop_validOperations)
-- main = quickCheck prop_validOperations
main = specs >> check
