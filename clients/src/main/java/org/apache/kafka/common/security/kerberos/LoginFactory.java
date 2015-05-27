/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.kerberos;

import org.apache.kafka.common.security.AuthUtils;
import javax.security.auth.Subject;
import java.io.IOException;
import javax.security.auth.login.LoginException;


public class LoginFactory {
    private Login login;
    private String serviceName;
    private String loginContext;

    public LoginFactory(String loginContext) throws IOException, LoginException {
        this.loginContext = loginContext;
        login = new Login(loginContext);
        login.startThreadIfNeeded();
        serviceName = AuthUtils.getJaasConfig(loginContext, AuthUtils.SERVICE_NAME);
    }

    public Subject subject() {
        return login.getSubject();
    }

    public String serviceName() {
        return serviceName;
    }

    public void close() {
        if (login != null) {
            login.shutdown();
        }
    }
}
