{-# LANGUAGE OverloadedStrings #-}
module Network.Gearman.Client (connectGearman, submitJob, submitJobHigh, submitJobLow, submitJobBg) where

import qualified Control.Monad.State as S
import qualified Data.ByteString.Char8 as B
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import qualified Data.HashMap.Strict as H
import qualified Data.Pool as Pool

import Network.Gearman.Protocol
import Network.Gearman.Internal

connectGearman :: B.ByteString -> HostName -> Port -> IO (Either GearmanError GearmanClient)
connectGearman i h p = do
  addrInfo <- getAddrInfo Nothing (Just h) (Just $ show p)

  sk <- socket (addrFamily (head addrInfo)) Stream defaultProtocol
  connect sk (addrAddress (head addrInfo))

  pool <- mkPool addrInfo

  _ <- return $ writePacket sk (mkRequest SET_CLIENT_ID i) >> response sk
  return $ Right $ GearmanClient sk pool (Just i) H.empty

  where mkPool :: [AddrInfo] -> IO (Pool.Pool Socket)
        mkPool a = Pool.createPool (mkConn a) sClose 1 10 10

        mkConn :: [AddrInfo] -> IO Socket
        mkConn a = do
            sk <- socket (addrFamily (head a)) Stream defaultProtocol
            connect sk (addrAddress (head a))
            return sk

responseOrError :: (Either GearmanError Packet) -> Gearman (Either GearmanError B.ByteString)
responseOrError pk = case pk of
    Left p  -> return $ Left p
    Right p -> let _:r:_ = B.split '\0' $ _msg p in return $ Right r

submitJob :: Function -> B.ByteString -> Gearman (Either GearmanError B.ByteString)
submitJob f d =  (submit $ B.concat [f, "\0\0", d]) >>= responseOrError

submitJobHigh :: Function -> B.ByteString -> Gearman (Either GearmanError B.ByteString)
submitJobHigh f d = do
    env <- S.get 
    Pool.withResource (_pool env) $ \s -> do
        writePacket s (mkRequest SUBMIT_JOB_HIGH req) >> response s >>= responseOrError
    where req = B.concat [f, "\0\0", d]

submitJobLow :: Function -> B.ByteString -> Gearman (Either GearmanError B.ByteString)
submitJobLow f d = do
    env <- S.get
    Pool.withResource (_pool env) $ \s -> do
        writePacket s (mkRequest SUBMIT_JOB_LOW req) >> response s >>= responseOrError
    where req = B.concat[f, "\0\0", d]

submitJobBg :: Function -> B.ByteString -> Gearman ()
submitJobBg f d = do
    env <- S.get
    Pool.withResource (_pool env) $ \s ->
        writePacket s (mkRequest SUBMIT_JOB_BG req) >> readPacket s >> return ()
    where req = B.concat [f, "\0\0", d]

submit :: B.ByteString -> Gearman (Either GearmanError Packet)
submit req = S.get >>= \env -> Pool.withResource (_pool env) $ \s ->
        writePacket s (mkRequest SUBMIT_JOB req) >> response s

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


