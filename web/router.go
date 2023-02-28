package web

import (
	"github.com/gin-gonic/gin"
	"go-mysql-cdc/global"
	"go-mysql-cdc/service"
	"log"
	"net/http"
)

var _server *http.Server

func Start() error {
	//if !global.Cfg().EnableWebAdmin { //哨兵
	//	return nil
	//}
	//
	//gin.SetMode(gin.ReleaseMode)
	//g := gin.New()
	////statics := "D:\\statics"
	////index := "D:\\statics\\index.html"
	//
	//statics := "statics"
	//index := path.Join(statics, "index.html")
	//g.Static("/statics", statics)
	//g.LoadHTMLFiles(index)
	//g.GET("/", webAdminFunc)
	//
	//port := global.Cfg().WebAdminPort
	//listen := fmt.Sprintf(":%s", strconv.Itoa(port))
	//_server = &http.Server{
	//	Addr:           listen,
	//	Handler:        g,
	//	ReadTimeout:    10 * time.Second,
	//	WriteTimeout:   10 * time.Second,
	//	MaxHeaderBytes: 1 << 20,
	//}
	//
	//ok, err := nets.IsUsableTcpAddr(listen)
	//if !ok {
	//	return err
	//}
	//
	//log.Println(fmt.Sprintf("Web Admin Listen At %s", listen))
	//go func() {
	//	if err := _server.ListenAndServe(); err != nil {
	//		logs.Error(err.Error())
	//	}
	//}()
	//
	//return nil
	httpLogHandleFunc := func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic info is: %v", err)
			}
		}()

		// 请求方式
		reqMethod := c.Request.Method
		// 请求路由
		//reqUri := c.Request.RequestURI
		//// 状态码
		//statusCode := c.Writer.Status()
		//// 请求IP
		//clientIP := c.ClientIP()
		//

		//跨域处理https://www.jianshu.com/p/1f34d602dbc4
		origin := c.Request.Header.Get("Origin") //请求头部
		if origin != "" {
			c.Header("Access-Control-Allow-Origin", "*")
			//服务器支持的所有跨域请求的方法
			c.Header("Access-Control-Allow-Methods", "POST,GET,OPTIONS,PUT,DELETE,UPDATE")
			//允许跨域设置可以返回其他子段，可以自定义字段
			c.Header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization,X-Access-Token,X-Application-Name,X-Request-Send-Time")
			// 允许浏览器（客户端）可以解析的头部 （重要）
			c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Cache-Control, Content-Language, Content-Type")
			//允许客户端传递校验信息比如 cookie (重要)
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		if reqMethod == "OPTIONS" {
			c.JSON(http.StatusOK, "ok")
		}
		c.Next()
	}
	//对外开放端口
	go func() {
		router := gin.Default()
		router.Use(httpLogHandleFunc)
		r1 := router.Group("/status")
		r1.GET("", GetStatus)
		r2 := router.Group("/config")
		r2.GET("info", GetInfo)
		router.Run("0.0.0.0:7000")
	}()
	return nil
}
func GetStatus(c *gin.Context) {
	cstatus := global.Status{}
	cstatus.UpdateCount = global.CanalStatus.UpdateCount
	cstatus.InsertCount = global.CanalStatus.InsertCount
	cstatus.DDLCount = global.CanalStatus.DDLCount
	cstatus.DeleteCount = global.CanalStatus.DeleteCount
	if p, err := service.TransferServiceIns().Position(); err == nil {
		cstatus.BinlogFile = p.Name
		cstatus.BinlogPos = p.Pos
	}
	c.JSON(http.StatusOK, cstatus)
}
func GetInfo(c *gin.Context) {
	c.JSON(http.StatusOK, global.Cfg())
}
