{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

module Thrift.Transport.Framed
    ( module Thrift.Transport
    , openFramedTransport
    ) where

import Thrift.Transport
import Thrift.Transport.IOBuffer

import Data.Int (Int32)
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as LBS

-- | Create a new framed transport which wraps the given transport.
openFramedTransport :: Transport -> IO Transport
openFramedTransport trans = do
  wbuf <- newWriteBuffer
  rbuf <- newReadBuffer
  let
    self = Transport
      { tIsOpen = tIsOpen trans
      , tClose = tClose trans
      , tRead = \n -> do
          -- First, check the read buffer for any data.
          bs <- readBuf rbuf n
          if LBS.null bs
             then
             -- When the buffer is empty, read another frame from the
             -- underlying transport.
               do len <- readFrame trans rbuf
                  if len > 0
                     then tRead self n
                     else return bs
             else return bs
      , tPeek = do
          mw <- peekBuf rbuf
          case mw of
            Just _ -> return mw
            Nothing -> do
              len <- readFrame trans rbuf
              if len > 0
                 then tPeek self
                 else return Nothing
      , tWrite = writeBuf wbuf
      , tFlush =  do
          bs <- flushBuf wbuf
          let szBs = B.encode $ (fromIntegral $ LBS.length bs :: Int32)
          tWrite trans szBs
          tWrite trans bs
          tFlush trans
      }
  return self

readFrame :: Transport -> ReadBuffer -> IO Int
readFrame trans rbuf = do
  -- Read and decode the frame size.
  szBs <- tRead trans 4
  let sz = fromIntegral (B.decode szBs :: Int32)

  -- Read the frame and stuff it into the read buffer.
  bs <- tRead trans sz
  fillBuf rbuf bs

  -- Return the frame size so that the caller knows whether to expect
  -- something in the read buffer or not.
  return sz
