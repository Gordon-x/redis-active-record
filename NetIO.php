<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/30
 * Time: 下午2:26
 */

namespace RedisActiveRecord;


class NetIO
{
    private $conn = null;
    private $command = null;

    public function __construct(Connection $conn)
    {
        $this->conn = $conn;
    }

    public function init()
    {

    }

    public function execute()
    {
    }

    private function response()
    {
    }
}
