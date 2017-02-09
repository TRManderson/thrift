{-# LANGUAGE FlexibleInstances #-}
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

module Thrift.Transport.HttpClient
    ( module Thrift.Transport
    , openHttpClient
) where

import Thrift.Transport
import Thrift.Transport.IOBuffer
import Network.URI
import Network.HTTP hiding (port, host)

import Data.Maybe (fromJust)
import Data.Monoid (mempty)
import Control.Exception (throw)
import qualified Data.ByteString.Lazy as LBS


-- | 'HttpClient', or THttpClient implements the Thrift Transport
-- | Layer over http or https.

uriAuth :: URI -> URIAuth
uriAuth = fromJust . uriAuthority

host :: URI -> String
host = uriRegName . uriAuth

port :: URI -> Int
port uri_ =
    if portStr == mempty then
        httpPort
    else
        read portStr
    where
      portStr = dropWhile (== ':') $ uriPort $ uriAuth uri_
      httpPort = 80

-- | Use 'openHttpClient' to create an HttpClient connected to @uri@
openHttpClient :: URI -> IO Transport
openHttpClient uri_ = do
  stream <- openTCPConnection (host uri_) (port uri_) :: IO (HandleStream LBS.ByteString)
  wbuf <- newWriteBuffer
  rbuf <- newReadBuffer
  let
    self = Transport
      { tClose = close stream
      , tPeek = peekBuf rbuf
      , tRead = readBuf rbuf
      , tWrite = writeBuf wbuf
      , tFlush = do
          body <- flushBuf wbuf
          let request = Request {
                          rqURI = uri_,
                          rqHeaders = [
                           mkHeader HdrContentType "application/x-thrift",
                           mkHeader HdrContentLength $  show $ LBS.length body],
                          rqMethod = POST,
                          rqBody = body
                        }
          res <- sendHTTP stream request
          case res of
            Right response ->
              fillBuf rbuf (rspBody response)
            Left _ ->
              throw $ TransportExn "THttpConnection: HTTP failure from server" TE_UNKNOWN
          return ()
      , tIsOpen = return True
      }
  return self
