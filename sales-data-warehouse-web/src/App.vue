<template>
  <div id="app">
    <a-config-provider :locale="zhCN">
      <router-view/>
    </a-config-provider>
  </div>
</template>

<script>
import zhCN from 'ant-design-vue/lib/locale-provider/zh_CN'

export default {
  name: 'App',
  data() {
    return {
      zhCN
    }
  }
}
</script>

<style>
#app {
  font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #2c3e50;
  height: 100%;
}

html, body {
  height: 100%;
  margin: 0;
  padding: 0;
}
</style> 