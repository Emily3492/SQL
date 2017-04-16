--cart                                   购物车
--cart_detail                            购物车详情表
--cart_merchant_coupon_selected          购物车优惠券被选择表
--commerce_coupon                        优惠券表
--commerce_coupon_range                  优惠券使用范围表
--commerce_coupon_user                   优惠券使用人表
--commerce_groupon                       团购表
--commerce_groupon_detail                团购明细表


--community                              小区表
--customer_service                       客服表
--department                             部门表
--evaluate                               评价表
--fixed_time_trigger                     定时时间触发表
--goods                                  商品表
--goods_audit                            商品审核表
--goods_category                         商品分类
--goods_pic                              商品图片表
--goods_release_community                商品发布小区表
--goods_restrict_time                    商品限购时间表
--goods_stocks_operation_record          商品库存操作记录表
--layout                                 版面管理表
--layout_release_community               版面发布小区
--menu                                   菜单表
--merchant                               商家表
-- merchant_category_privileges          商家商品分类权限表
--merchant_commerce_activity             商家营销活动
--merchant_commerce_activity_detail      商家营销活动详情';
--merchant_community_privileges          商家小区权限表
--merchant_user                          商户用户关系表
--message_push                           消息推送表
--new_old_system_community_relationship  新老系统小区关系对应表
--new_old_system_user_relationship       新老系统用户关系对应表


--order_detail                           订单详情表
--order_operate_record                   订单操作记录表
--order_package                          订单包裹表
--order_package_detail                   订单包裹详情表
--order_receiver_address                 订单收货地址
--order_table                            订单表
--role                                   角色表
--role_menu                              角色权限表
--user                                   用户表
--user_community                         用户小区表
--user_role                              用户角色表
--user_wallet                            用户钱包表







create database JH_Server_Commerce

use JH_Server_Commerce


/*==============================================================*/
/* Table: cart   购物车                                                */
/*==============================================================*/
create table cart
(
   cart_id              bigint(20) not null comment '购物车id',
   user_id              bigint(20) comment '用户id',
   user_name            varchar(200) comment '用户姓名',
   user_phone           varchar(200) comment '用户手机号',
   total_cost           double comment '总费用',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   remark               varchar(500) comment '备注',
   primary key (cart_id)
);

alter table cart comment '购物车';


/*==============================================================*/
/* Table: cart_detail    购物车详情表                                       */
/*==============================================================*/
create table cart_detail
(
   id                   bigint(20) not null comment '主键id',
   cart_id              bigint(20) comment '购物车id',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(200) comment '商品名称',
   goods_num            double comment '商品数量',
   merchant_id          bigint(20) comment '商品所属商家id',
   merchant_name        varchar(500) comment '商品所属商家名称',
   stores_name          varchar(500) comment '商品所属店铺名称',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   remark               varchar(500) comment '备注',
   primary key (id)
);

alter table cart_detail comment '购物车详情表';


/*==============================================================*/
/* Table: cart_merchant_coupon_selected   购物车优惠券被选择表                  */
/*==============================================================*/
create table cart_merchant_coupon_selected
(
   id                   bigint(20) not null comment '主键',
   cart_id              bigint(20) comment '购物车id',
   merchant_id          bigint(20) comment '商家id',
   coupon_id            bigint(20) comment '优惠券id',
   primary key (id)
);

alter table cart_merchant_coupon_selected comment '购物车优惠券被选择表';


/*==============================================================*/
/* Table: commerce_coupon       优惠券表                                */
/*==============================================================*/
create table commerce_coupon
(
   coupon_id            bigint(20) not null comment '优惠券id',
   coupon_name          varchar(500) comment '优惠券名称',
   activity_start_time  datetime comment '活动开始时间',
   activity_end_time    datetime comment '活动结束时间',
   use_rule             varchar(50) comment '使用规则: 0-无限制(消费满0.01元即可使用)   1-限定消费金额',
   discount_amount      double comment '折扣金额',
   consumption_amount   double comment '消费金额达到多少时满足使用优惠券',
   free_postage         char(1) default 'Y' comment '不含邮费:： Y是，N否',
   number               int comment '生成数量',
   use_range_type       int comment '使用范围类型： 0-可以全场   1-按照频道   3-某部分商品',
   mod_time             datetime comment '修改时间',
   remark               varchar(5000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   primary key (coupon_id)
);

alter table commerce_coupon comment '优惠券表';


/*==============================================================*/
/* Table: commerce_coupon_range       优惠券使用范围表                          */
/*==============================================================*/
create table commerce_coupon_range
(
   id                   bigint(20) not null comment '主键id',
   coupon_id            bigint(20) comment '优惠券id',
   coupon_name          varchar(500) comment '优惠券名称',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(500) comment '商品名称',
   merchant_id          bigint(20) comment '商品所属商家id',
   merchant_name        varchar(500) comment '商品所属商家名称',
   stores_name          varchar(500) comment '商品所属店铺名称',
   remark               varchar(5000) comment '备注',
   primary key (id)
);

alter table commerce_coupon_range comment '优惠券使用范围表';


/*==============================================================*/
/* Table: commerce_coupon_user           优惠券使用人表                       */
/*==============================================================*/
create table commerce_coupon_user
(
   id                   bigint(20) not null comment '主键id',
   coupon_id            bigint(20) not null comment '优惠券id',
   coupon_name          varchar(500) comment '优惠券名称',
   coupon_code          varchar(200) comment '优惠券编码',
   user_id              bigint(20) comment '用户id',
   user_name            varchar(200) comment '用户姓名',
   user_phone           varchar(200) comment '用户手机号',
   use_time             datetime comment '使用时间',
   use_order_code       varchar(200) comment '使用订单编号',
   remark               varchar(5000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   send_time            datetime comment '发放时间',
   status               int(11) comment '状态：0 未使用，1 已使用，2 冻结',
   primary key (id)
);

alter table commerce_coupon_user comment '优惠券使用人表';


/*==============================================================*/
/* Table: commerce_groupon            团购表                          */
/*==============================================================*/
create table commerce_groupon
(
   groupon_id           bigint(20) not null comment '团购id',
   activity_name        varchar(500) comment '活动名称',
   activity_start_time  datetime comment '活动开始时间',
   activity_end_time    datetime comment '活动结束时间',
   discount_type        varchar(50) comment '折扣类型:0-统一折扣 1-部分折扣',
   discount_percentage  double comment '统一折扣(%)',
   mod_time             datetime comment '修改时间',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   remark               varchar(5000) comment '备注',
   primary key (groupon_id)
);

alter table commerce_groupon comment '团购表';


/*==============================================================*/
/* Table: commerce_groupon_detail      团购明细表                         */
/*==============================================================*/
create table commerce_groupon_detail
(
   id                   bigint(20) not null comment '主键id',
   groupon_id           bigint(20) comment '团购id',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(500) comment '商品名称',
   discount_percentage  double comment '折扣(%)',
   groupon_cost         double comment '团购价(元)',
   sortno               int comment '排序号',
   merchant_id          bigint(20) comment '商品所属商家id',
   merchant_name        varchar(500) comment '商品所属商家名称',
   stores_name          varchar(500) comment '商品所属店铺名称',
   activity_start_time  datetime comment '活动开始时间',
   activity_end_time    datetime comment '活动结束时间',
   remark               varchar(5000) comment '备注',
   primary key (id)
);

alter table commerce_groupon_detail comment '团购明细表';


/*==============================================================*/
/* Table: community                    小区表                         */
/*==============================================================*/
create table community
(
   community_id         bigint(20) not null comment '小区id',
   community_name       varchar(500) comment '小区名称',
   sort_no              int comment '排序号',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   remark               varchar(5000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   old_community_id     varchar(200) comment '对应老系统小区id',
   community_type       varchar(100) comment '小区类型：garden-悦园区',
   primary key (community_id)
);

alter table community comment '小区表';


/*==============================================================*/
/* Table: department           部门表                               */
/*==============================================================*/
create table department
(
   id                   bigint(20) not null comment '主键',
   dept_sid             varchar(200) comment '对应老系统部门id',
   group_sid            varchar(200) comment '对应老系统物业集团SID',
   dept_name            varchar(400) comment '部门名称',
   parent_dept_id       varchar(200) comment '对应老系统上级部门',
   primary key (id)
);

alter table department comment '部门表';


/*==============================================================*/
/* Table: evaluate         评价表                               */
/*==============================================================*/
create table evaluate
(
   id                   bigint(20) not null,
   goods_id             bigint(20) comment '商品id',
   order_id             bigint(20) comment '订单id',
   order_no             varchar(200) comment '订单号',
   evaluater_id         bigint(20) comment '评价人id',
   evaluater_name       varchar(100) comment '评价人姓名',
   evaluater_phone      varchar(100) comment '评价人手机号',
   evaluate_time        datetime comment '评价时间',
   evaluate_result      varchar(100) comment '评价结果：1-好评   2-中评   3-差评',
   evaluate_content     varchar(1000) comment '评价内容',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   remark               varchar(5000) comment '备注',
   is_shield            char(1) default 'N' comment '是否屏蔽：Y是，N否',
   primary key (id)
);

alter table evaluate comment '评价表';


/*==============================================================*/
/* Table: fixed_time_trigger       定时时间触发表                     */
/*==============================================================*/
create table fixed_time_trigger
(
   id                   bigint(20) not null comment '主键id',
   time_unit            varchar(200) comment '时间单位：minute-分钟   hour-小时   day-天',
   time_value           int comment '值大小',
   time_type            varchar(200) comment '类型：autoConfirmReceiveGoods-自动收货   autoBestEvaluate-自动评价  autoCancelOrder-自动取消订单',
   primary key (id)
);

alter table fixed_time_trigger comment '定时时间触发表';


/*==============================================================*/
/* Table: fixed_time_trigger2       定时时间触发表               */
/*==============================================================*/
create table fixed_time_trigger2
(
   id                   bigint(20) not null comment '主键id',
   time_unit            varchar(200) comment '时间单位：minute-分钟   hour-小时   day-天',
   time_value           int comment '值大小',
   time_type            varchar(200) comment '类型：autoConfirmReceiveGoods-自动收货   autoBestEvaluate-自动评价  autoCancelOrder-自动取消订单',
   primary key (id)
);

alter table fixed_time_trigger2 comment '定时时间触发表';


/*==============================================================*/
/* Table: goods             商品表                              */
/*==============================================================*/
create table goods
(
   goods_id             bigint(20) not null comment '商品id',
   goods_code           varchar(100) comment '商品编码',
   goods_name           varchar(200) comment '商品名称',
   category_id          bigint(20) comment '所属分类id',
   merchant_id          bigint(20) comment '所属商家id',
   merchant_name        varchar(500) comment '所属商家名称',
   stores_name          varchar(500) comment '所属店铺名称',
   buying_price         double comment '进货价(元)',
   retail_price         double comment '零售价(元)',
   num                  double comment '库存数量',
   min_sales_num        double comment '起卖数量',
   max_sales_num        double comment '限购数量',
   distribution_mode    int comment '配送方式：1-物业配送  2- 快递配送',
   distribution_cost    double comment '配送费用(元)',
   goods_status         char(1) default '1' comment '商品状态：1-下架  2-申请上架  3-上架  9-上架被拒',
   shelves_time         datetime comment '上架时间',
   apply_shelves_time   datetime comment '申请上架时间',
   is_new_push          varchar(200) default 'N' comment '是否上新推送:Y是，N否',
   goods_details        blob comment '商品详情',
   goods_label          varchar(200) comment '商品标签',
   label_color          varchar(200) comment '标签颜色',
   sales_type           int comment '经营分类：1-自营商品  2-商户商品',
   is_top               int default 0 comment '是否置顶：1-是，0-否',
   top_time             datetime comment '置顶时间',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   sort_no              int default 0 comment '排序号',
   goods_release_type   varchar(20) comment '商品发布类型: all-全部小区   parts:部分小区',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (goods_id)
);

alter table goods comment '商品表';


/*==============================================================*/
/* Table: goods_audit        商品审核表                           */
/*==============================================================*/
create table goods_audit
(
   id                   bigint(20) not null comment 'id',
   goods_id             bigint(20) not null comment '商品id',
   audit_result         varchar(20) comment '审核结果',
   refuse_reason        varchar(2000) comment '拒绝原因',
   audit_user_id        bigint(20) comment '审核人',
   audit_time           datetime comment '审核时间',
   primary key (id)
);

alter table goods_audit comment '商品审核表';


/*==============================================================*/
/* Table: goods_category       商品分类                         */
/*==============================================================*/
create table goods_category
(
   id                   bigint(20) not null comment 'id',
   pid                  bigint(20) comment '父分类id',
   categoryNo           varchar(100) comment '分类编码',
   category_name        varchar(100) comment '分类名称',
   isDisable            char(1) default 'N' comment '是否禁用：Y是，N否',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   sort_no              int comment '排序号',
   primary key (id)
);

alter table goods_category comment '商品分类';


/*==============================================================*/
/* Table: goods_pic        商品图片表                            */
/*==============================================================*/
create table goods_pic
(
   id                   bigint(20) not null comment 'id',
   goods_id             bigint(20) not null comment '商品id',
   url                  varchar(500) comment '图片url',
   description          varchar(1000) comment '图片描述',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (id)
);

alter table goods_pic comment '商品图片表';


/*==============================================================*/
/* Table: goods_release_community       商品发布小区表               */
/*==============================================================*/
create table goods_release_community
(
   id                   bigint(20) not null comment 'id',
   goods_id             bigint(20) not null comment '商品id',
   community_id         bigint(20) comment '小区id',
   community_name       varchar(500) comment '小区名称',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (id)
);

alter table goods_release_community comment '商品发布小区表';


/*==============================================================*/
/* Table: goods_stocks_operation_record     商品库存操作记录表      */
/*==============================================================*/
create table goods_stocks_operation_record
(
   id                   bigint(20) not null comment 'id',
   goods_id             bigint(20) not null comment '商品id',
   num                  double comment '库存操作数量：为负数表示减少多少库存，为正数表示增加多少库存',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (id)
);

alter table goods_stocks_operation_record comment '商品库存操作记录表';


/*==============================================================*/
/* Table: layout           版面管理表                           */
/*==============================================================*/
create table layout
(
   layout_id            bigint(20) not null comment '主键id',
   layout_type          varchar(20) comment '版面类型：1-banner  2-菜单 3-广告位',
   layout_name          varchar(100) comment '版面名称',
   pic_url              varchar(200) comment '图片url',
   forward_type         int comment '跳转类型:1-内部跳转  2-URL跳转  3-图文详情跳转',
   forward_url          varchar(200) comment '跳转url',
   forward_graphic_details blob comment '跳转图文详情',
   inside_forword_type  varchar(100) comment '内部跳转类型：1-商户  2-频道  3-商品  4-团购',
   inside_forword_id    bigint(20) comment '内部跳转id：商户id/频道id/商品id/团购id',
   inside_forword_name  varchar(200) comment '内部跳转名称',
   sequenceNo           int comment '序列号',
   useful_start_time    datetime comment '有效开始时间',
   useful_end_time      datetime comment '有效结束时间',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   remark               varchar(1000) comment '备注',
   isdisable            char(1) default 'N' comment '是否禁用：Y是，N否',
   primary key (layout_id)
);

alter table layout comment '版面管理表';


/*==============================================================*/
/* Table: layout_release_community      版面发布小区               */
/*==============================================================*/
create table layout_release_community
(
   id                   bigint(20) not null comment '主键id',
   layout_id            bigint(20) comment '版面id',
   community_id         double comment '小区id',
   community_name       varchar(200) comment '小区名称',
   primary key (id)
);

alter table layout_release_community comment '版面发布小区';


/*==============================================================*/
/* Table: menu        菜单表                                    */
/*==============================================================*/
create table menu
(
   menu_id              bigint(20) not null comment '菜单id',
   pid                  varchar(100) not null comment '父菜单ID',
   name                 varchar(100) comment '菜单名称',
   sort                 varchar(100) not null comment '排序号',
   url                  varchar(100) comment '链接地址',
   icon                 varchar(100) comment '菜单图标',
   remark               varchar(1000) comment '备注',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (menu_id)
);

alter table menu comment '菜单表';


/*==============================================================*/
/* Table: merchant         商家表                               */
/*==============================================================*/
create table merchant
(
   merchant_id          bigint(20) not null comment '商家id',
   merchant_name        varchar(500) comment '商家名称',
   merchant_code        varchar(200) comment '商户编码：默认四位数，自1001起，保留0000-0999另做他用(1000作为自营商家编码)',
   stores_name          varchar(500) comment '店铺名称',
   phone                varchar(200) comment '联系电话',
   status               varchar(20) default '1' comment '商家状态:  0-启用   1-停用',
   merchant_type        varchar(20) default '0' comment '商家类型:  1-自营  2-商户',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   sort_no              int default 0 comment '排序号',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   remark               varchar(2000) comment '备注',
   primary key (merchant_id)
);

alter table merchant comment '商家表';


/*==============================================================*/
/* Table: merchant_category_privileges      商家商品分类权限表         */
/*==============================================================*/
create table merchant_category_privileges
(
   id                   bigint(20) not null comment 'id',
   merchant_id          bigint(20) comment '商家id',
   category_id          bigint(20) comment '商品分类id',
   category_name        varchar(2000) comment '分类名称',
   remark               varchar(2000) comment '备注',
   primary key (id)
);

alter table merchant_category_privileges comment '商家商品分类权限表';


/*==============================================================*/
/* Table: merchant_commerce_activity       商家营销活动            */
/*==============================================================*/
create table merchant_commerce_activity
(
   activity_id          bigint(20) not null comment '活动id',
   activity_name        varchar(500) comment '活动名称',
   activity_start_time  datetime comment '活动开始时间',
   activity_end_time    datetime comment '活动结束时间',
   money_num_mail       double comment '满多少元可以包邮',
   merchant_id          bigint(20) comment '活动所属商家id',
   merchant_name        varchar(500) comment '商家名称',
   stores_name          varchar(500) comment '商家店铺名称',
   remark               varchar(5000) comment '备注',
   mod_time             datetime comment '修改时间',
   mod_user_id          bigint(20) comment '修改用户id',
   isdel                varchar(1) comment '是否删除：Y 是，N 否',
   primary key (activity_id)
);

alter table merchant_commerce_activity comment '商家营销活动';


/*==============================================================*/
/* Table: merchant_commerce_activity_detail    商家营销活动详情    */
/*==============================================================*/
create table merchant_commerce_activity_detail
(
   id                   bigint(20) not null comment '主键id',
   activity_id          bigint(20) comment '活动id',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(200) comment '商品名称',
   primary key (id)
);

alter table merchant_commerce_activity_detail comment '商家营销活动详情';


/*==============================================================*/
/* Table: merchant_community_privileges      商家小区权限表       */
/*==============================================================*/
create table merchant_community_privileges
(
   id                   bigint(20) not null comment 'id',
   merchant_id          bigint(20) comment '商家id',
   community_id         bigint(20) comment '小区id',
   community_name       varchar(500) comment '小区名称',
   remark               varchar(5000) comment '备注',
   primary key (id)
);

alter table merchant_community_privileges comment '商家小区权限表';


/*==============================================================*/
/* Table: merchant_user           商户用户关系表                   */
/*==============================================================*/
create table merchant_user
(
   user_id              bigint(20) comment '用户id',
   merchant_id          bigint(20) comment '商户id',
   account              varchar(200) comment '账号',
   password             varchar(200) comment '密码',
   user_name            varchar(200) comment '用户姓名',
   phone                varchar(200) comment '手机号码',
   remark               varchar(2000) comment '备注',
   isenabled            varchar(200) default '1' comment '０：停用，１：启用',
   create_time          datetime comment '创建时间'
);

alter table merchant_user comment '商户用户关系表';


/*==============================================================*/
/* Table: message_push           消息推送表                       */
/*==============================================================*/
create table message_push
(
   id                   bigint(20) not null,
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(500) comment '商品名称',
   apply_push_time      datetime comment '申请推送时间',
   apply_rejected_time  datetime comment '申请被拒时间',
   push_status          varchar(50) comment '推送状态：0-申请推送  1-待推送   2-已过期    3-申请被拒',
   push_content         varchar(5000) comment '推送内容',
   push_time            datetime comment '推送时间',
   is_apply             char(1) comment '是否为申请：Y是，N否',
   refuse_reason        varchar(2000) comment '被拒原因',
   remark               varchar(5000) comment '备注',
   primary key (id)
);

alter table message_push comment '消息推送表';


/*==============================================================*/
/* Table: new_old_system_community_relationship       新老系统小区关系对应表          */
/*==============================================================*/
create table new_old_system_community_relationship
(
   id                   bigint(20) not null comment '主键id',
   community_id         bigint(20) comment '小区id',
   old_community_id     varchar(200) comment '老系统对应的小区id',
   remark               varchar(500) comment '备注',
   primary key (id)
);

alter table new_old_system_community_relationship comment '新老系统小区关系对应表';


/*==============================================================*/
/* Table: new_old_system_user_relationship       新老系统用户关系对应表               */
/*==============================================================*/
create table new_old_system_user_relationship
(
   id                   bigint(20) not null comment '主键id',
   user_id              bigint(20) comment '用户id',
   old_user_id          varchar(200) comment '老系统对应的用户id',
   remark               varchar(500) comment '备注',
   primary key (id)
);

alter table new_old_system_user_relationship comment '新老系统用户关系对应表';


/*==============================================================*/
/* Table: order_detail         订单详情表                                 */
/*==============================================================*/
create table order_detail
(
   id                   bigint(20) not null comment 'id',
   order_id             bigint(20) comment '订单id',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(200) comment '商品名称',
   goods_num            double comment '商品数量',
   retail_price         double comment '零售价(元)',
   discount_percentage  double comment '折扣(%)',
   current_price        double comment 'd',
   primary key (id)
);

alter table order_detail comment '订单详情表';


/*==============================================================*/
/* Table: order_package           订单包裹表                              */
/*==============================================================*/
create table order_package
(
   package_id           bigint(20) not null comment '包裹id',
   package_name         varchar(200) comment '包裹名称',
   order_id             bigint(20) comment '订单id',
   order_no             varchar(200) comment '订单号',
   distribution_mode    int comment '配送方式：1-物业配送  2- 快递配送',
   goods_deliver_user   varchar(500) comment '发货人',
   goods_deliver_time   datetime comment '发货时间',
   goods_deliver_user_phohe varchar(200) comment '发货人手机',
   express_company      varchar(200) comment '快递公司',
   express_no           varchar(200) comment '快递单号',
   primary key (package_id)
);

alter table order_package comment '订单包裹表';


/*==============================================================*/
/* Table: order_package_detail          订单包裹详情表                        */
/*==============================================================*/
create table order_package_detail
(
   id                   bigint(20) not null comment 'id',
   package_id           bigint(20) comment '包裹id',
   order_id             bigint(20) comment '订单id',
   order_no             varchar(200) not null comment '订单号',
   goods_id             bigint(20) comment '商品id',
   goods_name           varchar(200) comment '商品名称',
   goods_num            double comment '商品数量',
   primary key (id)
);

alter table order_package_detail comment '订单包裹详情表';


/*==============================================================*/
/* Table: order_receiver_address         订单收货地址                       */
/*==============================================================*/
create table order_receiver_address
(
   id                   bigint(20) not null comment '主键id',
   user_id              bigint(20) comment '下单人用户id',
   receiver_name        varchar(200) comment '收货人姓名',
   receiver_phone       varchar(200) comment '收货人手机号',
   receiver_area        varchar(500) comment '收货人所在地区',
   receiver_detail_address varchar(500) comment '收货人详细地址',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   is_default_address   char(1) default 'N' comment '是否默认地址:Y-是  N-否',
   primary key (id)
);

alter table order_receiver_address comment '订单收货地址';


/*==============================================================*/
/* Table: order_table         订单表                                  */
/*==============================================================*/
create table order_table
(
   order_id             bigint(20) not null comment '订单id',
   order_no             varchar(200) comment '订单号',
   pay_no               varchar(200) comment '支付流水号',
   order_user_id        bigint(20) comment '下单人',
   order_time           datetime comment '下单时间',
   goods_receiver_id    bigint(20) comment '收货人id',
   goods_receiver_name  varchar(200) comment '收货人姓名',
   goods_receiver_phone varchar(200) comment '收货人手机号码',
   goods_receiver_address varchar(500) comment '收货地址',
   goods_receiver_time  datetime comment '收货时间',
   receive_remark       varchar(1000) comment '收货备注',
   order_status         varchar(20) comment '订单状态: 1-待付款  2-待发货 3-待收货 4-确认收货  5-待评价 6-已经完成    -1：已经失效(取消或者自动取消)',
   order_remarks        varchar(2000) comment '订单备注',
   total_amount         double comment '订单总金额(元)',
   cancel_order_reason  varchar(1000) comment '取消订单原因',
   distribution_mode    int comment '配送方式：1-物业配送  2- 快递配送',
   distribution_cost    double comment '配送费用(元)',
   merchant_id          bigint(20) comment '商品所属商家id',
   merchant_name        varchar(500) comment '商品所属商家名称',
   stores_name          varchar(500) comment '商品所属店铺名称',
   coupon_id            bigint(20) comment '使用的优惠券id',
   coupon_amount        double comment '使用的优惠券金额(元)',
   actual_pay_amount    double comment '除去优惠券总金额(元)',
   primary key (order_id)
);

alter table order_table comment '订单表';


/*==============================================================*/
/* Table: role          角色表                                        */
/*==============================================================*/
create table role
(
   role_id              bigint(20) not null comment '角色ID',
   role_name            varchar(100) not null comment '角色名称',
   remark               varchar(1000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   primary key (role_id)
);

alter table role comment ' 角色表';


/*==============================================================*/
/* Table: role_menu            角色权限表                                 */
/*==============================================================*/
create table role_menu
(
   id                   bigint(20) not null comment 'id',
   role_id              varchar(100) not null comment '角色id',
   menu_id              varchar(100) comment '菜单ID',
   primary key (id)
);

alter table role_menu comment '角色权限表';


/*==============================================================*/
/* Table: user            用户表                                      */
/*==============================================================*/
create table user
(
   user_id              bigint(20) not null comment '用户id',
   user_account         varchar(100) comment '业主房号／物业用户帐号',
   user_name            varchar(100) comment '用户姓名',
   password             varchar(100) comment '密码',
   user_mobile          varchar(100) comment '手机号',
   head_url             varchar(500) comment '头像url',
   user_tel             varchar(100) comment '电话',
   user_email           varchar(200) comment '邮箱',
   community_id         bigint(20) comment '所属小区id',
   community_name       varchar(200) comment '小区名称',
   remark               varchar(1000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   create_time          datetime comment '创建时间',
   create_user_id       bigint(20) comment '创建人',
   mod_user_id          bigint(20) comment '修改人',
   mod_time             datetime comment '修改时间',
   nick_name            varchar(400) comment '昵称',
   old_community_id     varchar(400) comment '对应老系统小区id',
   old_user_id          varchar(400) comment '对应老系统用户id,用于对接老系统',
   user_type            varchar(200) comment '用户类型 ：1 - 小区用户   2 - 小区服务人员  3 - 集团服务人员   4 - 小区管理员   5 - 集团管理员  garden-悦园区',
   group_sid            varchar(200) comment '老系统集团SID',
   department_id        bigint(20) comment '部门id',
   old_department_id    varchar(200) comment '对应老系统部门id',
   owner_status         int comment '用户状态（０：停用，１：启用）',
   owner_tag            varchar(200) comment '职务',
   owner_category       int comment '业主类型（0：业主，1：租户，2：家属）3.普通用户，4业主',
   primary key (user_id)
);

alter table user comment '用户表';


/*==============================================================*/
/* Table: user_community          用户小区表                              */
/*==============================================================*/
create table user_community
(
   old_id               varchar(50) not null comment '老的主键id',
   old_user_id          varchar(50) comment '对应老系统用户id',
   old_community_id     varchar(50) comment '对应老系统小区id',
   primary key (old_id)
);

alter table user_community comment '用户小区表';


/*==============================================================*/
/* Table: user_role            用户角色表                                 */
/*==============================================================*/
create table user_role
(
   id                   bigint(20) not null comment 'id',
   user_id              varchar(100) not null comment '用户id',
   role_id              varchar(100) comment '角色id',
   primary key (id)
);

alter table user_role comment '用户角色表';


/*==============================================================*/
/* Table: user_wallet            用户钱包表                               */
/*==============================================================*/
create table user_wallet
(
   id                   bigint(20) not null comment '主键id',
   user_id              bigint(20) comment '用户id',
   user_name            varchar(200) comment '用户姓名',
   coupon_id            bigint(20) comment '优惠券id',
   coupon_name          bigint(20) comment '优惠券名称',
   activity_start_time  datetime comment '活动开始时间',
   activity_end_time    datetime comment '活动结束时间',
   mod_time             datetime comment '修改时间',
   remark               varchar(5000) comment '备注',
   isdel                char(1) default 'N' comment '是否删除：Y是，N否',
   primary key (id)
);

alter table user_wallet comment '用户钱包表';

alter table cart_detail add constraint FK_Reference_16 foreign key (cart_id)
      references cart (cart_id) on delete restrict on update restrict;

alter table commerce_coupon_range add constraint FK_Reference_15 foreign key (coupon_id)
      references commerce_coupon (coupon_id) on delete restrict on update restrict;

alter table commerce_coupon_user add constraint FK_Reference_14 foreign key (coupon_id)
      references commerce_coupon (coupon_id) on delete restrict on update restrict;

alter table commerce_groupon_detail add constraint FK_Reference_13 foreign key (groupon_id)
      references commerce_groupon (groupon_id) on delete restrict on update restrict;

alter table goods_audit add constraint FK_Reference_3 foreign key (goods_id)
      references goods (goods_id) on delete restrict on update restrict;

alter table goods_pic add constraint FK_Reference_2 foreign key (goods_id)
      references goods (goods_id) on delete restrict on update restrict;

alter table goods_release_community add constraint FK_Reference_1 foreign key (goods_id)
      references goods (goods_id) on delete restrict on update restrict;

alter table merchant_category_privileges add constraint FK_Reference_8 foreign key (merchant_id)
      references merchant (merchant_id) on delete restrict on update restrict;

alter table merchant_community_privileges add constraint FK_Reference_7 foreign key (merchant_id)
      references merchant (merchant_id) on delete restrict on update restrict;

alter table order_detail add constraint FK_Reference_4 foreign key (order_id)
      references order_table (order_id) on delete restrict on update restrict;

alter table order_package add constraint FK_Reference_5 foreign key (order_id)
      references order_table (order_id) on delete restrict on update restrict;

alter table order_package_detail add constraint FK_Reference_6 foreign key (package_id)
      references order_package (package_id) on delete restrict on update restrict;

alter table role_menu add constraint FK_Reference_11 foreign key (role_id)
      references role (role_id) on delete restrict on update restrict;

alter table role_menu add constraint FK_Reference_12 foreign key (menu_id)
      references menu (menu_id) on delete restrict on update restrict;

alter table user_role add constraint FK_Reference_10 foreign key (role_id)
      references role (role_id) on delete restrict on update restrict;

alter table user_role add constraint FK_Reference_9 foreign key (user_id)
      references user (user_id) on delete restrict on update restrict;
