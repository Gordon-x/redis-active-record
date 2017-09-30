<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/30
 * Time: 上午11:20
 */

namespace RedisActiveRecord;


use yii\base\ErrorException;

class StreamConnection implements Connection
{
    public $host = 'localhost';
    public $port = 6379;
    public $database = 0;
    public $auth = null;
    public $connectionTimeout = 60;
    public $dataTimeout = 60;
    public $socketClientFlags = STREAM_CLIENT_CONNECT;

    private $conn = null;

    public function connection()
    {
        if ($this->isActive()) {
            return true;
        }
        $this->conn = stream_socket_client(
            'tcp://' . $this->host . ':' . $this->port,
            $error_no,
            $err_msg,
            $this->connectionTimeout,
            $this->socketClientFlags
        );
        if ($this->isActive()) {
            return true;
        }
        throw new ErrorException('Connection Failed! MSG:'.$error_no.';'.$err_msg);
    }

    public function auth()
    {
        // TODO: Implement auth() method.
    }

    public function selectDB()
    {
        // TODO: Implement selectDB() method.
    }

    public function close()
    {
        if (!$this->isActive()) {
            //TODO QUIT
            stream_socket_shutdown($this->conn, STREAM_SHUT_RDWR);
            $this->conn = null;
        }
    }

    public function isActive()
    {
        return $this->conn !== null;
    }

    public function send(Command $command)
    {
        $stream_contents = $command->getCommands();
        fwrite($this->conn, $stream_contents);
        return $this;
    }

    public function accept(Command $command)
    {
        $stream_contents = $command->getCommands();
        return $this->response($stream_contents);
    }

    private function response($commands)
    {
        if (($line = fgets($this->conn)) === false) {
            throw new ErrorException('读取数据失败，Command:'.$commands);
        }
    }
}
