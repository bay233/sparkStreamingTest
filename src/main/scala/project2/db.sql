-- ----------------------------
--  Table structure for `user_words`
-- ----------------------------
DROP TABLE IF EXISTS `user_words`;
CREATE TABLE `user_words` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `word` varchar(100) NOT NULL COMMENT '统计关键词',
  `add_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '添加时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `user_words`
-- ----------------------------
BEGIN;
 INSERT INTO `user_words` VALUES  (id, '炮', '2018-07-31 21:30:24');
 INSERT INTO `user_words` VALUES  (id, '穷', '2018-07-31 21:30:44');
 INSERT INTO `user_words` VALUES  (id, '虾', '2018-07-31 21:31:37');
 INSERT INTO `user_words` VALUES  (id, '狗', '2018-07-31 21:31:47');
 INSERT INTO `user_words` VALUES  (id, '氦', '2018-07-31 21:34:02');
 INSERT INTO `user_words` VALUES  (id, '狼', '2018-07-31 21:34:08');
 INSERT INTO `user_words` VALUES  (id, '玖', '2018-07-31 21:35:52');
 INSERT INTO `user_words` VALUES  (id, '酒', '2018-07-31 21:38:21');
 INSERT INTO `user_words` VALUES  (id, '涨', '2018-07-31 21:38:36');
 INSERT INTO `user_words` VALUES  (id, '盔', '2018-07-31 21:39:22');
COMMIT;

select * from user_words;

show tables;

select * from word_count_202006

-- ----------------------------
--  Table structure for `word_count_201808`
-- ----------------------------
DROP TABLE IF EXISTS `word_count_201808`;
CREATE TABLE `word_count_201808` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(64) NOT NULL,
  `count` int(11) DEFAULT '0',
  `date` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `word` (`word`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=3584 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `word_count_201808`
-- ----------------------------
BEGIN;
INSERT INTO `word_count_201808` VALUES ('1', '穷', '22', '2018-08-05'), ('2', '炮', '7', '2018-08-05'), ('3', '酒', '24', '2018-08-05'), ('4', '狗', '44', '2018-08-05'), ('6', '玖', '34', '2018-08-05'), ('7', '虾', '63', '2018-08-05'), ('8', '盔', '32', '2018-08-05'), ('10', '狼', '32', '2018-08-05'), ('11', '涨', '11', '2018-08-05'), ('14', '氦', '71', '2018-08-05'), ('278', '狼', '651', '2018-08-19'), ('279', '穷', '270', '2018-08-19'), ('280', '虾', '688', '2018-08-19'), ('281', '玖', '466', '2018-08-19'), ('286', '盔', '491', '2018-08-19'), ('287', '氦', '968', '2018-08-19'), ('291', '涨', '202', '2018-08-19'), ('293', '炮', '133', '2018-08-19'), ('297', '狗', '504', '2018-08-19'), ('308', '酒', '296', '2018-08-19'), ('555', '甜蜜', '1', '2018-08-19'), ('1770', '哈哈', '1', '2018-08-19'), ('3583', '森森', '1', '2018-08-19');
COMMIT;

-- ----------------------------
--  Table structure for `word_count_201811`
-- ----------------------------
DROP TABLE IF EXISTS `word_count_201811`;
CREATE TABLE `word_count_201811` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `word` varchar(64) NOT NULL,
  `count` int(11) DEFAULT '0',
  `date` date NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `word` (`word`,`date`)
) ENGINE=InnoDB AUTO_INCREMENT=87 DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `word_count_201811`
-- ----------------------------
BEGIN;
INSERT INTO `word_count_201811` VALUES ('1', '穷', '10', '2018-11-18'), ('2', '虾', '26', '2018-11-18'), ('3', '氦', '33', '2018-11-18'), ('7', '狼', '24', '2018-11-18'), ('8', '狗', '16', '2018-11-18'), ('9', '盔', '29', '2018-11-18'), ('11', '涨', '7', '2018-11-18'), ('12', '玖', '24', '2018-11-18'), ('14', '酒', '8', '2018-11-18'), ('46', '炮', '6', '2018-11-18'), ('86', '书籍', '1', '2018-11-18');
COMMIT;