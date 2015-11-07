module Main
    ( main
    ) where


-------------------------------------------------------------------------------
import           Test.Tasty
-------------------------------------------------------------------------------
import qualified Database.Bloodhound.Tests.Extras
-------------------------------------------------------------------------------


main :: IO ()
main = defaultMain tests



tests :: TestTree
tests = testGroup "bloodhound-extras"
  [
    Database.Bloodhound.Tests.Extras.tests
  ]
