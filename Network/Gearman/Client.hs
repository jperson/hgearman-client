{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Network.Gearman.Client (connectGearman, submitJob, submitJobBg) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Trans
import           Control.Monad.Trans.Loop
import qualified Control.Monad.State as S
import           Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashTable.IO as H
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import           Network.Socket.ByteString
import qualified Data.HashMap.Strict as H
import           Data.Maybe

import Network.Gearman.Protocol
import Network.Gearman.Internal

data JobResult = JobResult {
    _data :: B.ByteString
  , _handle :: B.ByteString
} deriving (Show)

type Function = B.ByteString

connectGearman :: B.ByteString -> HostName -> Port -> IO (Either GearmanError GearmanClient)
connectGearman i h p = do
  addrInfo <- getAddrInfo Nothing (Just h) (Just $ show p)

  sk <- socket (addrFamily (head addrInfo)) Stream defaultProtocol
  connect sk (addrAddress (head addrInfo))

  return $ (writePacket sk $ mkRequest SET_CLIENT_ID i) >> response sk
  return $ Right $ GearmanClient sk (Just i) $ H.empty

submitJob :: Function -> B.ByteString -> Gearman (Either GearmanError JobResult)
submitJob f d = do
    packet <- submit $ B.concat [f, "\0\0", d]
    case packet of
        Left p  -> return $ Left p
        Right p -> let [h,r] = B.split '\0' $ _msg p in return $ Right $ JobResult r h

submit :: B.ByteString -> Gearman (Either GearmanError Packet)
submit req = S.get >>= \env -> (writePacket (_sock env) $ mkRequest SUBMIT_JOB req) >> response (_sock env)

response :: Socket -> Gearman (Either GearmanError Packet)
response s = do
    p <- readPacket s
    case (_type . _hdr) p of
        WORK_COMPLETE   -> return $ Right p
        WORK_DATA       -> return $ Right p
        WORK_STATUS     -> return $ Right p
        WORK_EXCEPTION  -> return $ Left "WORK_EXCEPTION"
        WORK_FAIL       -> return $ Left "WORK_FAIL"
        WORK_WARNING    -> return $ Left "WORK_WARNING"
        _               -> response s

submitJobBg :: Function -> B.ByteString -> Gearman ()
submitJobBg f d = S.get >>= \env ->
    (writePacket (_sock env) $ (mkRequest SUBMIT_JOB_BG req)) >> readPacket (_sock env) >> return ()
    where req = B.concat [f, "\0\0", d]

