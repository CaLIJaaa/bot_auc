CREATE TABLE IF NOT EXISTS `user` (
	id BIGINT auto_increment NOT NULL,
	tg_id BIGINT UNIQUE NOT NULL,
	tg_link varchar(100) NOT NULL,
	lang ENUM('ru', 'en') NOT NULL,
	status ENUM('user', 'admin', 'banned') NOT NULL,
	CONSTRAINT user_pk PRIMARY KEY (id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `auction` (
	id BIGINT auto_increment NOT NULL,
	created_by BIGINT NOT NULL,
	picture varchar(257) NULL,
	name varchar(100) NOT NULL,
	`type` ENUM('wine', 'whiskey') NOT NULL,
	volume BIGINT NULL,
	abv BIGINT NULL,
	country varchar(100) NULL,
	brand varchar(100) NULL,
	produser varchar(100) NULL,
	description varchar(200) NULL,
	price FLOAT NULL,
	time_start TIMESTAMP NULL,
	time_leinght TIME NOT NULL,
	status ENUM('created', 'opened', 'closed', 'deleted') NOT NULL,
	CONSTRAINT auction_pk PRIMARY KEY (id),
	CONSTRAINT auction_user_FK FOREIGN KEY (created_by) REFERENCES `user`(id) ON DELETE RESTRICT ON UPDATE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `bid` (
	id BIGINT auto_increment NOT NULL,
	user_id BIGINT NOT NULL,
	auction_id BIGINT NOT NULL,
	money FLOAT NOT NULL,
	time_bid TIMESTAMP NOT NULL,
	CONSTRAINT bid_pk PRIMARY KEY (id),
	CONSTRAINT bid_user_FK FOREIGN KEY (user_id) REFERENCES `user`(id) ON DELETE RESTRICT ON UPDATE CASCADE,
	CONSTRAINT bid_auction_FK FOREIGN KEY (auction_id) REFERENCES `auction`(id) ON DELETE RESTRICT ON UPDATE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;
