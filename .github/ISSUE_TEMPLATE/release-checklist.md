---
name: Release checklist
about: 跟踪一次版本发布
title: "[Release] "
labels: release
assignees: ""
---

- [ ] `npm run bootstrap:windows`
- [ ] `npm run doctor:windows`
- [ ] `npm run check`
- [ ] `npm run build`
- [ ] `npm run package:windows`
- [ ] 安装验证
- [ ] 覆盖升级验证
- [ ] 卸载验证
- [ ] `CHANGELOG.md` 已更新
- [ ] Release Notes 已更新
