module Main
    ( main
    ) where


-------------------------------------------------------------------------------
import           Data.Proxy
import           Test.Tasty
import           Test.Tasty.Options
-------------------------------------------------------------------------------
import qualified Database.Bloodhound.Tests.Extras
-------------------------------------------------------------------------------


main :: IO ()
main = defaultMainWithIngredients ings tests
  where
    ings = (includingOptions [Option (Proxy :: Proxy Database.Bloodhound.Tests.Extras.TestWithESVersion)]):defaultIngredients



tests :: TestTree
tests = testGroup "bloodhound-extras"
  [
    Database.Bloodhound.Tests.Extras.tests
  ]
