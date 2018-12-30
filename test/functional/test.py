import mahler.core.registrar

from register import tags


registrar = mahler.core.registrar.build(name='mongodb')


for i, task in enumerate(registrar.retrieve_tasks(tags)):
    print(i, task.name, task.id)
