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

module Thrift.Transport.Memory
       ( openMemoryBuffer
       ) where

import Data.ByteString.Lazy.Builder
import Data.Functor
import Data.IORef
import Data.Monoid
import qualified Data.ByteString.Lazy as LBS

import Thrift.Transport

openMemoryBuffer :: IO Transport
openMemoryBuffer = do
  wbuf <- newIORef mempty
  rbuf <- newIORef mempty
  let
    trans = Transport
      { tIsOpen = return False
      , tClose  = return ()
      , tFlush = do
          wb <- readIORef wbuf
          modifyIORef rbuf $ \rb -> mappend rb $ toLazyByteString wb
          writeIORef wbuf mempty
      , tRead = \n -> if n == 0 then return mempty else do
          rb <- readIORef rbuf
          let len = fromIntegral $ LBS.length rb
          if len == 0
            then do
              tFlush trans
              rb2 <- readIORef rbuf
              if (fromIntegral $ LBS.length rb2) == 0
                then return mempty
                else tRead trans n
            else do
              let (ret, remain) = LBS.splitAt (fromIntegral n) rb
              writeIORef rbuf remain
              return ret
      , tPeek = (fmap fst . LBS.uncons) <$> readIORef rbuf
      , tWrite = \v -> modifyIORef wbuf (<> lazyByteString v)
      }
  return trans
