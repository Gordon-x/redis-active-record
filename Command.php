<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/30
 * Time: 上午11:22
 */

namespace RedisActiveRecord;


class Command
{
    private $command = '';

    /**
     * 生成命令
     * @param $commandName
     * @param array $param
     */
    public function create($commandName, array $param = [])
    {
        $params = array_merge(explode(' ', $commandName), $param);
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }
        $this->command = $command;
    }

    /**
     * 获取命令；
     * @return mixed
     */
    public function getCommands()
    {
        return $this->command;
    }
}
