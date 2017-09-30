<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/30
 * Time: 上午11:23
 */

namespace RedisActiveRecord;


class Client
{
    const DEFAULT_CONNECTION = 'stream';

    public $connection = self::DEFAULT_CONNECTION;

    private static $conn_map = [
        self::DEFAULT_CONNECTION => StreamConnection::class
    ];

    private $db = null;

    public function __construct()
    {
        $conn = self::$conn_map[$this->connection] ?? self::$conn_map[self::DEFAULT_CONNECTION];
        $this->db = new NetIO(new $conn);
    }

    public function __call($name, $arguments)
    {
    }
}
