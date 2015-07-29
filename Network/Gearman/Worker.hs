{-# LANGUAGE OverloadedStrings #-}
module Network.Gearman.Worker(registerWorker, unregisterWorker, runWorker) where

import           Control.Concurrent
import           Control.Monad
import qualified Control.Monad.State as S
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as H

import Network.Gearman.Protocol
import Network.Gearman.Internal

runWorker :: GearmanClient -> Gearman () -> IO ThreadId
runWorker c f = forkIO $ withGearman c (f >> gmLoop)

registerWorker :: Function -> (B.ByteString -> B.ByteString) -> Gearman ()
registerWorker n f = S.get >>= \env -> do
    let ht = H.insert n f (_fns env)
    S.put env{ _fns = ht }
    void $ writePacket (_sock env) (mkRequest CAN_DO n)

unregisterWorker :: Function -> Gearman ()
unregisterWorker n = S.get >>= \env -> do
    let ht = H.delete n (_fns env)
    S.put env { _fns = ht }

    void $ writePacket (_sock env) (mkRequest CANT_DO n)

gmWait :: Gearman ()
gmWait = S.get >>= \env -> do
    packet <- readPacket (_sock env)
    case (_type . _hdr) packet of
        NO_JOB          -> writePacket (_sock env) (mkRequest PRE_SLEEP "") >> gmWait
        JOB_ASSIGN      -> doJob (_msg packet) >>= \p -> void $ writePacket (_sock env) p
        NOOP            -> return ()
        _               -> gmWait

gmLoop :: Gearman ()
gmLoop = S.get >>= \env -> forever $ writePacket (_sock env) (mkRequest GRAB_JOB "") >> gmWait

doJob :: B.ByteString -> Gearman Packet
doJob m = S.get >>= \env -> do
    let [h,f,d] = B.split '\0' m
    return $ case H.lookup f (_fns env) of
        Just fn -> let resp = B.concat [h, "\0", fn d] in mkRequest WORK_COMPLETE resp
        Nothing -> mkRequest WORK_FAIL ""
