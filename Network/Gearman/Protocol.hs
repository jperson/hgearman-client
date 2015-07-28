{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Network.Gearman.Protocol ( Packet(..)
                                , PacketHdr(..)
                                , MsgType(..)
                                , readPacket
                                , writePacket
                                , mkRequest
                                ) where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Trans
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Control.Monad.State as S
import           Control.Monad.IO.Class
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashTable.IO as H
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Network.Socket hiding (send, sendTo, recv, recvFrom)
import           Network.Socket.ByteString
import           Network.Socket.Options
import qualified Data.HashMap.Strict as H
import           Data.Maybe

import Network.Gearman.Internal

data MsgType =
      UNUSED_BEGIN
    | CAN_DO
    | CANT_DO
    | RESET_ABILITIES
    | PRE_SLEEP
    | UNUSED
    | NOOP
    | SUBMIT_JOB
    | JOB_CREATED
    | GRAB_JOB
    | NO_JOB
    | JOB_ASSIGN
    | WORK_STATUS
    | WORK_COMPLETE
    | WORK_FAIL
    | GET_STATUS
    | ECHO_REQ
    | ECHO_RES
    | SUBMIT_JOB_BG
    | ERROR
    | STATUS_RES
    | SUBMIT_JOB_HIGH
    | SET_CLIENT_ID
    | CAN_DO_TIMEOUT
    | ALL_YOURS
    | WORK_EXCEPTION
    | OPTION_REQ
    | OPTION_RES
    | WORK_DATA
    | WORK_WARNING
    | GRAB_JOB_UNIQ
    | JOB_ASSIGN_UNIQ
    | SUBMIT_JOB_HIGH_BG
    | SUBMIT_JOB_LOW
    | SUBMIT_JOB_LOW_BG
    | SUBMIT_JOB_SCHED
    | SUBMIT_JOB_EPOCH
    deriving (Show, Eq, Enum)


data PacketHdr = PacketHdr {
                   _magic :: !B.ByteString
                  ,_type  :: !MsgType
                  ,_size  :: !Word32
                 } deriving (Show, Eq)

data Packet = Packet {
                _hdr  :: !PacketHdr
               ,_msg  :: !B.ByteString
} deriving (Show, Eq)

instance Binary PacketHdr where
    get = do
        magic <- getByteString 4
        ptype <- getWord32be
        size  <- getWord32be
        return $ PacketHdr magic (toEnum $ fromIntegral ptype) size

    put (PacketHdr magic ptype size) = do
        putByteString magic
        putWord32be (fromIntegral $ fromEnum ptype)
        putWord32be size

instance Binary Packet where
    get = do
        hdr <- get :: Get PacketHdr
        msg  <- getByteString $ fromIntegral (_size hdr)
        return $ Packet hdr msg

    put (Packet hdr msg) = do
        put hdr
        putByteString msg

recv' :: Socket -> Int -> IO B.ByteString
recv' _ 0 = return ""
recv' s n = do
    msg <- recv s n
    case n - B.length msg of
        0 -> return msg
        otherwise -> recv' s (n - B.length msg) >>= \x -> return $ msg `B.append` x

readPacket :: Socket -> Gearman Packet
readPacket = readSocket

readSocket ::  Socket -> Gearman Packet
readSocket s = do
    hdr <- liftIO $ recv' s 12
    let h = decode $ BL.fromStrict hdr
    msg <- liftIO $ recv' s (fromIntegral $ _size h)
    return $ Packet h msg

writePacket :: Socket -> Packet -> Gearman ()
writePacket s p = do
    liftIO $ send s $ (BL.toStrict . encode) p
    return ()

mkRequest :: MsgType -> B.ByteString -> Packet
mkRequest t d = Packet (PacketHdr "\0REQ" t (fromIntegral $ B.length d)) d
