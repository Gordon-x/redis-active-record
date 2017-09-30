<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/30
 * Time: 上午11:33
 */

namespace RedisActiveRecord;


use Yii;
use yii\base\InvalidConfigException;
use yii\db\BaseActiveRecord;
use yii\helpers\Inflector;
use yii\helpers\StringHelper;

class ActiveRecord extends BaseActiveRecord
{
    public static function getDb()
    {
        return Yii::$app->get('redis_active_record');
    }

    public static function find()
    {
        return Yii::createObject(ActiveQuery::class, [get_called_class()]);
    }

    public static function primaryKey()
    {
        return ['id'];
    }

    public function attributes()
    {
        throw new InvalidConfigException('The attributes() method of redis ActiveRecord has to be implemented by child classes.');
    }

    public static function keyPrefix()
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '_');
    }

    /**
     * @inheritdoc
     */
    public function insert($runValidation = true, $attributes = null)
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        if (!$this->beforeSave(true)) {
            return false;
        }
        $db = static::getDb();
        $values = $this->getDirtyAttributes($attributes);
        $pk = [];
        foreach ($this->primaryKey() as $key) {
            $pk[$key] = $values[$key] = $this->getAttribute($key);
            if ($pk[$key] === null) {
                // use auto increment if pk is null
                $pk[$key] = $values[$key] = $db->executeCommand('INCR', [static::keyPrefix() . ':s:' . $key]);
                $this->setAttribute($key, $values[$key]);
            } elseif (is_numeric($pk[$key])) {
                // if pk is numeric update auto increment value
                $currentPk = $db->executeCommand('GET', [static::keyPrefix() . ':s:' . $key]);
                if ($pk[$key] > $currentPk) {
                    $db->executeCommand('SET', [static::keyPrefix() . ':s:' . $key, $pk[$key]]);
                }
            }
        }
        // save pk in a findall pool
        $pk = static::buildKey($pk);
        $db->executeCommand('RPUSH', [static::keyPrefix(), $pk]);

        $key = static::keyPrefix() . ':a:' . $pk;
        // save attributes
        $setArgs = [$key];
        foreach ($values as $attribute => $value) {
            // only insert attributes that are not null
            if ($value !== null) {
                if (is_bool($value)) {
                    $value = (int) $value;
                }
                $setArgs[] = $attribute;
                $setArgs[] = $value;
            }
        }

        if (count($setArgs) > 1) {
            $db->executeCommand('HMSET', $setArgs);
        }

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);

        return true;
    }

    public static function updateAll($attributes, $condition = null)
    {
        if (empty($attributes)) {
            return 0;
        }
        $db = static::getDb();
        $n = 0;
        foreach (self::fetchPks($condition) as $pk) {
            $newPk = $pk;
            $pk = static::buildKey($pk);
            $key = static::keyPrefix() . ':a:' . $pk;
            // save attributes
            $delArgs = [$key];
            $setArgs = [$key];
            foreach ($attributes as $attribute => $value) {
                if (isset($newPk[$attribute])) {
                    $newPk[$attribute] = $value;
                }
                if ($value !== null) {
                    if (is_bool($value)) {
                        $value = (int) $value;
                    }
                    $setArgs[] = $attribute;
                    $setArgs[] = $value;
                } else {
                    $delArgs[] = $attribute;
                }
            }
            $newPk = static::buildKey($newPk);
            $newKey = static::keyPrefix() . ':a:' . $newPk;
            // rename index if pk changed
            if ($newPk != $pk) {
                $db->executeCommand('MULTI');
                if (count($setArgs) > 1) {
                    $db->executeCommand('HMSET', $setArgs);
                }
                if (count($delArgs) > 1) {
                    $db->executeCommand('HDEL', $delArgs);
                }
                $db->executeCommand('LINSERT', [static::keyPrefix(), 'AFTER', $pk, $newPk]);
                $db->executeCommand('LREM', [static::keyPrefix(), 0, $pk]);
                $db->executeCommand('RENAME', [$key, $newKey]);
                $db->executeCommand('EXEC');
            } else {
                if (count($setArgs) > 1) {
                    $db->executeCommand('HMSET', $setArgs);
                }
                if (count($delArgs) > 1) {
                    $db->executeCommand('HDEL', $delArgs);
                }
            }
            $n++;
        }

        return $n;
    }

    public static function updateAllCounters($counters, $condition = null)
    {
        if (empty($counters)) {
            return 0;
        }
        $db = static::getDb();
        $n = 0;
        foreach (self::fetchPks($condition) as $pk) {
            $key = static::keyPrefix() . ':a:' . static::buildKey($pk);
            foreach ($counters as $attribute => $value) {
                $db->executeCommand('HINCRBY', [$key, $attribute, $value]);
            }
            $n++;
        }

        return $n;
    }

    /**
     * Deletes rows in the table using the provided conditions.
     * WARNING: If you do not specify any condition, this method will delete ALL rows in the table.
     *
     * For example, to delete all customers whose status is 3:
     *
     * ~~~
     * Customer::deleteAll(['status' => 3]);
     * ~~~
     *
     * @param array $condition the conditions that will be put in the WHERE part of the DELETE SQL.
     * Please refer to [[ActiveQuery::where()]] on how to specify this parameter.
     * @return int the number of rows deleted
     */
    public static function deleteAll($condition = null)
    {
        $pks = self::fetchPks($condition);
        if (empty($pks)) {
            return 0;
        }

        $db = static::getDb();
        $attributeKeys = [];
        $db->executeCommand('MULTI');
        foreach ($pks as $pk) {
            $pk = static::buildKey($pk);
            $db->executeCommand('LREM', [static::keyPrefix(), 0, $pk]);
            $attributeKeys[] = static::keyPrefix() . ':a:' . $pk;
        }
        $db->executeCommand('DEL', $attributeKeys);
        $result = $db->executeCommand('EXEC');

        return end($result);
    }

    private static function fetchPks($condition)
    {
        $query = static::find();
        $query->where($condition);
        $records = $query->asArray()->all(); // TODO limit fetched columns to pk
        $primaryKey = static::primaryKey();

        $pks = [];
        foreach ($records as $record) {
            $pk = [];
            foreach ($primaryKey as $key) {
                $pk[$key] = $record[$key];
            }
            $pks[] = $pk;
        }

        return $pks;
    }

    /**
     * Builds a normalized key from a given primary key value.
     *
     * @param mixed $key the key to be normalized
     * @return string the generated key
     */
    public static function buildKey($key)
    {
        if (is_numeric($key)) {
            return $key;
        } elseif (is_string($key)) {
            return ctype_alnum($key) && StringHelper::byteLength($key) <= 32 ? $key : md5($key);
        } elseif (is_array($key)) {
            if (count($key) == 1) {
                return self::buildKey(reset($key));
            }
            ksort($key); // ensure order is always the same
            $isNumeric = true;
            foreach ($key as $value) {
                if (!is_numeric($value)) {
                    $isNumeric = false;
                }
            }
            if ($isNumeric) {
                return implode('-', $key);
            }
        }

        return md5(json_encode($key, JSON_NUMERIC_CHECK));
    }
}
