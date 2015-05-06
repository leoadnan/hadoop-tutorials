/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.examples.ch02;

import java.sql.Date;

/**
 *
 * @author aahmed
 */
public class WeblogRecord {

    private String cookie;
    private String page;
    private Date date;
    private String ip;

    public WeblogRecord() {
    }

    public WeblogRecord(String cookie, String page, Date date, String ip) {
        this.cookie = cookie;
        this.page = page;
        this.date = date;
        this.ip = ip;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    
    @Override
    public String toString() {
        return cookie + "\t" + page + "\t" + date.toString() + "\t" + ip;
    }
}
