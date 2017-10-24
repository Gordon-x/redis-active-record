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
    /**
     * @var Connection
     */
    private $conn;

    public function __construct(Connection $conn)
    {
        $this->conn = $conn;
    }

    public function __set($name, $value)
    {
        $this->conn->$name = $value;
    }

    public function connect()
    {
        $this->conn->connection();
    }

    public function execute(Command $command)
    {
        $this->conn->send($command);
        return $this->response($command);
    }

    private function response(Command $command)
    {
        return $this->conn->accept($command);
    }
}
