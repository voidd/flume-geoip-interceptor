/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ru.jilime.flume;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeoIPExtractor  implements Interceptor {

    public static final String HOST_DFLT = "ipAddress";
    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DFLT = false;
    public static final String HOST_HEADER = "addressHeader";
    public static final String GEOIP_DATABASE = "geoIPDatabase";

    private LookupService lookupService;

    private static final Logger logger = LoggerFactory
            .getLogger(GeoIPExtractor.class);

    private final boolean preserveExisting;
    private final String header;
    private String host = null;
    private static final Pattern IPV4_REGEX = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");


    private GeoIPExtractor(boolean preserveExisting, String header, LookupService lookupService) {
        this.preserveExisting = preserveExisting;
        this.header = header;
        this.lookupService = lookupService;
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Could not get local host address. Exception follows.", e);
        }


    }

    public void initialize() {
        // no-op
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        if (preserveExisting && headers.containsKey(header)) {
            appendEvent(event);
            return event;
        }
        if(host != null) {
            headers.put(header, host);
        }

        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
        // no-op
    }

    public void appendEvent(Event e) {
        Map<String, String> headers = new HashMap<String, String>();
        if (e.getHeaders().containsKey(HOST_HEADER)) {
            String ipAddress = e.getHeaders().get(HOST_HEADER);
            if (isValidIp(ipAddress)) {
                Location l = lookupService.getLocation(e.getHeaders().get(HOST_HEADER));
                if (l != null) {
                    String dstPrefix = "geoip";
                    if (l.city != null) {
                        headers.put(dstPrefix + ".city", l.city.trim());
                    }
                    if (l.countryName != null) {
                        headers.put(dstPrefix + ".countryName", l.countryName.trim());
                    }
                    if (l.countryCode != null) {
                        headers.put(dstPrefix + ".countryCode", l.countryCode.trim());
                    }
                    if (l.longitude != 0) {
                        headers.put(dstPrefix + ".longitude", Float.toString(l.longitude));
                    }
                    if (l.latitude != 0) {
                        headers.put(dstPrefix + ".latitude", Float.toString(l.latitude));
                    }
                    headers.putAll(e.getHeaders());
                }
            } else {
                logger.warn("Unable to parse attribute '" + HOST_HEADER + "' as an IP");
            }
        } else {
            logger.warn("Attribute '" + HOST_HEADER + "' not found in event");
        }

        // if appended headers is not empty put them in the Event object
        if (!headers.isEmpty()) {
            e.setHeaders(headers);
        }
    }

    private boolean isValidIp(String ipAddress) {
        if (StringUtils.isNotBlank(ipAddress)) {
            Matcher m = IPV4_REGEX.matcher(ipAddress);
            return m.matches();
        }
        return false;
    }

    /**
     * Builder which builds new instances of the GeoIPExtractor.
     */
    public static class Builder implements Interceptor.Builder {

        private boolean preserveExisting = PRESERVE_DFLT;
        private String header = HOST_DFLT;
        private String geoIpDatabase;

        public Interceptor build() {
            LookupService lookupService;
            try {
                File database = new File(geoIpDatabase);
                if (!database.exists()) {
                    throw new IllegalArgumentException("File '" + geoIpDatabase + "' does not exist");
                }
                lookupService = new LookupService(database, LookupService.GEOIP_MEMORY_CACHE);
            } catch (IOException e) {
                throw new IllegalArgumentException("File '" + geoIpDatabase + "' is not readable");
            }
            return new GeoIPExtractor(preserveExisting, header, lookupService);
        }

        public void configure(Context context) {
            geoIpDatabase = context.getString(GEOIP_DATABASE);
            if (geoIpDatabase == null || geoIpDatabase.trim().length() == 0) {
                throw new IllegalArgumentException("Missing parameter: " + GEOIP_DATABASE, null);
            }
            preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
            header = context.getString(HOST_HEADER, HOST_DFLT);

        }
    }
}
