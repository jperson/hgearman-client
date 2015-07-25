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

connectGearman :: B.ByteString -> HostName -> Port -> IO (Either GearmanError GearmanClient)
connectGearman i h p = do
  addrInfo <- getAddrInfo Nothing (Just h) (Just $ show p)

  sk <- socket (addrFamily (head addrInfo)) Stream defaultProtocol
  connect sk (addrAddress (head addrInfo))

  return $ (writePacket sk $ mkRequest SET_CLIENT_ID i) >> response sk
  return $ Right $ GearmanClient sk (Just i) $ H.empty

submitJob :: B.ByteString -> B.ByteString -> Gearman (B.ByteString)
submitJob f d = do
    packet <- submit $ B.concat [f, "\0\0", d]
    let [h,r] = B.split '\0' $ _msg packet
    return r


submit :: B.ByteString -> Gearman Packet
submit req = S.get >>= \env -> (writePacket (_sock env) $ mkRequest SUBMIT_JOB req) >> response (_sock env)

response :: Socket -> Gearman Packet
response s = do
    p <- readPacket s
    case (_type . _hdr) p of
        WORK_COMPLETE   -> return p
        WORK_DATA       -> return p
        WORK_STATUS     -> return p
        WORK_EXCEPTION  -> return p
        WORK_FAIL       -> return p
        WORK_WARNING    -> return p
        _               -> (liftIO $ (print . show ._type . _hdr) p) >> response s

submitJobBg :: B.ByteString -> B.ByteString -> Gearman ()
submitJobBg f d = S.get >>= \env ->
    (writePacket (_sock env) $ (mkRequest SUBMIT_JOB_BG req)) >> readPacket (_sock env) >> return ()
    where req = B.concat [f, "\0\0", d]

