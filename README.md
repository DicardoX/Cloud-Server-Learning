# Cloud-Server-Learning

&emsp; **本文档记录云主机创建过程中的问题**

------------------

## 域名解析错误

&emsp; 错误信息为`temporary failure resolving 'mirrors.aliyun,com'`

&emsp; 解决方案：

 - 获取一个可用的DNS服务器地址，可以查看本机的DNS服务器地址
 
 - 进入配置文件`/etc/netplan/`，打开配置文件（适用于`18.04`版本以上的Ubuntu），添加：
 
 ```
 nameservers:
     addresses : [[Your DNS address]]
 ```
 
 - 进入配置文件`/etc/resolv.conf`，添加相同的`nameserver [Your DNS address]`
 
 - 重启`reboot`

--------------------

## Mac连接云主机

[参考链接：Mac上传文件到Linux服务器](https://www.cnblogs.com/faunjoe88/p/8094068.html)

&emsp; **注意**！要使用浮动IP而非内网IP。
