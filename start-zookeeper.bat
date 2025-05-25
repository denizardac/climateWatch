     @echo off
     cd /d %~dp0
     .\zookeeper-server-start.bat ..\..\config\zookeeper.properties
     pause