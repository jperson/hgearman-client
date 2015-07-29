{-# LANGUAGE OverloadedStrings #-}
module Network.Gearman.Internal ( Port
                                , Function
                                , GearmanClient(..)
                                , GearmanError
                                , Gearman()
                                , withGearman
                                ) where

import qualified Control.Monad.State as S
import qualified Data.ByteString as B
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import qualified Data.HashMap.Strict as H
import qualified Data.Pool as Pool

type Port = Int
type Function = B.ByteString

data GearmanClient = GearmanClient {
    _sock :: Socket
  , _pool :: Pool.Pool Socket
  , _id   :: Maybe B.ByteString
  , _fns  :: H.HashMap B.ByteString (B.ByteString -> B.ByteString)
}

type GearmanError = B.ByteString
type Gearman = S.StateT GearmanClient IO

withGearman :: GearmanClient -> Gearman a -> IO a
withGearman env action = S.runStateT action env >>= (\(v,_) -> return v)
