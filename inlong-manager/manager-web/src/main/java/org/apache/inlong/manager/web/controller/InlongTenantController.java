/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.web.controller;

import org.apache.inlong.manager.common.enums.OperationType;
import org.apache.inlong.manager.common.validation.UpdateByIdValidation;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.tenant.InlongTenantInfo;
import org.apache.inlong.manager.pojo.tenant.InlongTenantPageRequest;
import org.apache.inlong.manager.pojo.tenant.InlongTenantRequest;
import org.apache.inlong.manager.service.operationlog.OperationLog;
import org.apache.inlong.manager.service.tenant.InlongTenantService;

import com.github.pagehelper.PageInfo;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Api(tags = "Tenant-API")
public class InlongTenantController {

    @Autowired
    private InlongTenantService tenantService;

    @RequestMapping(value = "/tenant/get/{name}", method = RequestMethod.GET)
    @ApiOperation(value = "Get tenant")
    @ApiImplicitParam(name = "name", dataTypeClass = String.class, required = true)
    public Response<InlongTenantInfo> get(@PathVariable String name) {
        return Response.success(tenantService.get(name));
    }

    @RequestMapping(value = "/tenant/save", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.CREATE)
    @ApiOperation(value = "Save tenant")
    public Response<Integer> save(@Validated @RequestBody InlongTenantRequest request) {
        return Response.success(tenantService.save(request));
    }

    @RequestMapping(value = "/tenant/list", method = RequestMethod.POST)
    @ApiOperation(value = "List tenant by paginating")
    public Response<PageInfo<InlongTenantInfo>> listByCondition(@RequestBody InlongTenantPageRequest request) {
        return Response.success(tenantService.listByCondition(request));
    }

    @RequestMapping(value = "/tenant/update", method = RequestMethod.POST)
    @OperationLog(operation = OperationType.UPDATE)
    @ApiOperation(value = "Update tenant")
    public Response<Boolean> update(@Validated(UpdateByIdValidation.class) @RequestBody InlongTenantRequest request) {
        return Response.success(tenantService.update(request));
    }

}
