{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Network.Gearman.Internal ( Port
                                , GearmanClient(..)
                                , GearmanError
                                , Gearman(..)
                                , withGearman
                                ) where

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad
import           Control.Monad.Base
import           Control.Monad.Trans
import           Control.Monad.Trans.Control
import qualified Control.Monad.State as S
import           Control.Monad.IO.Class
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashTable.IO as H
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import           Network.Socket.ByteString
import qualified Data.HashMap.Strict as H
import           Data.Maybe
import qualified Data.Pool as Pool

type Port = Int
data GearmanClient = GearmanClient {
    _sock :: Socket
  , _pool :: Pool.Pool Socket
  , _id   :: Maybe B.ByteString
  , _fns  :: H.HashMap B.ByteString (B.ByteString -> B.ByteString)
}

type GearmanError = B.ByteString
type Gearman = S.StateT GearmanClient IO

withGearman :: GearmanClient -> Gearman a -> IO a
withGearman env action = S.runStateT action env >>= (\(v,s) -> return v)
