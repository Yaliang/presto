/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.security;

import org.testng.annotations.Test;

import java.io.File;

public class TestKerberosAuthenticator
{
    @Test
    public void testK()
    {
        KerberosConfig config = new KerberosConfig()
                .setKerberosConfig(new File("/etc/krb5.conf"))
                //.setKeytab(new File("/Users/yaliangw/Downloads/presto.keytab"))
                .setServiceName("HTTP/presto-coordinator--devel--presto.service.smf1.twitter.biz@TWITTER.BIZ")
                .setCredentialCache(new File("/Users/yaliangw/Downloads/presto_service_krb5.cc"));
        KerberosAuthenticator m = new KerberosAuthenticator(config);
    }
}
