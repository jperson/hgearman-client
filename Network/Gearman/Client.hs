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
import qualified Data.Pool as Pool

import Network.Gearman.Protocol
import Network.Gearman.Internal

type Function = B.ByteString

connectGearman :: B.ByteString -> HostName -> Port -> IO (Either GearmanError GearmanClient)
connectGearman i h p = do
  addrInfo <- getAddrInfo Nothing (Just h) (Just $ show p)

  sk <- socket (addrFamily (head addrInfo)) Stream defaultProtocol
  connect sk (addrAddress (head addrInfo))

  pool <- mkPool addrInfo

  return $ (writePacket sk $ mkRequest SET_CLIENT_ID i) >> response sk
  return $ Right $ GearmanClient sk pool (Just i) $ H.empty

  where mkPool :: [AddrInfo] -> IO (Pool.Pool Socket)
        mkPool a = Pool.createPool (mkConn a) sClose 1 10 10

        mkConn :: [AddrInfo] -> IO Socket
        mkConn a = do
            sk <- socket (addrFamily (head a)) Stream defaultProtocol
            connect sk (addrAddress (head a))
            return sk

submitJob :: Function -> B.ByteString -> Gearman (Either GearmanError B.ByteString)
submitJob f d = do
    packet <- submit $ B.concat [f, "\0\0", d]
    case packet of
        Left p  -> return $ Left p
        Right p -> let [h,r] = B.split '\0' $ _msg p in return $ Right r

submit :: B.ByteString -> Gearman (Either GearmanError Packet)
submit req = S.get >>= \env -> do
    Pool.withResource (_pool env) $ \s -> do
        (writePacket s $ mkRequest SUBMIT_JOB req) >> response s

response :: Socket -> Gearman (Either GearmanError Packet)
response s = do
    p <- readPacket s
    case (_type . _hdr) p of
        WORK_COMPLETE   -> return $ Right p
        WORK_DATA       -> return $ Left "WORK_DATA NOT IMPLEMENTED" 
        WORK_STATUS     -> return $ Left "WORK_STATUS NOT IMPLEMENTED" 
        WORK_EXCEPTION  -> return $ Left "WORK_EXCEPTION NOT IMPLEMENTED"
        WORK_FAIL       -> return $ Left "WORK_FAIL NOT IMPLEMENTED"
        WORK_WARNING    -> return $ Left "WORK_WARNING NOT IMPLEMENTED"
        _               -> response s

submitJobBg :: Function -> B.ByteString -> Gearman ()
submitJobBg f d = S.get >>= \env -> do
    Pool.withResource (_pool env) $ \s -> do
        (writePacket s $ (mkRequest SUBMIT_JOB_BG req)) >> readPacket s >> return ()

    where req = B.concat [f, "\0\0", d]

