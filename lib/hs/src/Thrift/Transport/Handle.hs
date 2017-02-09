{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
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

module Thrift.Transport.Handle
    ( module Thrift.Transport
    , HandleSource(..)
    , handleTransport
    ) where

import Control.Exception ( catch, throw )
import Data.ByteString.Internal (c2w)
import Data.Functor

import Network

import System.IO
import System.IO.Error ( isEOFError )

import Thrift.Transport

import qualified Data.ByteString.Lazy as LBS
import Data.Monoid

handleTransport :: Handle -> Transport
handleTransport handle = Transport
  { tIsOpen = hIsOpen handle
  , tClose = hClose handle
  , tRead = \n -> LBS.hGet handle n `Control.Exception.catch` handleEOF mempty
  , tPeek = (Just . c2w <$> hLookAhead handle) `Control.Exception.catch` handleEOF Nothing
  , tWrite = LBS.hPut handle
  , tFlush = hFlush handle
  }

-- | Type class for all types that can open a Handle. This class is used to
-- replace tOpen in the Transport type class.
class HandleSource s where
    hOpen :: s -> IO Transport

instance HandleSource FilePath where
    hOpen s = handleTransport <$> openFile s ReadWriteMode

instance HandleSource (HostName, PortID) where
    hOpen = fmap handleTransport . uncurry connectTo


handleEOF :: a -> IOError -> IO a
handleEOF a e = if isEOFError e
    then return a
    else throw $ TransportExn "TChannelTransport: Could not read" TE_UNKNOWN
