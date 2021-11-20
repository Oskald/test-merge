package com.bamtechmedia.engage.ecommerceconsumer.component;

import com.bamtechmedia.engage.ecommerceconsumer.module.SendEmailModule;
import com.bamtechmedia.engage.ecommerceconsumer.module.UserServiceModule;
import com.bamtechmedia.engage.ecommerceconsumer.service.EcommerceProcessorService;
import dagger.Component;
import javax.inject.Singleton;

@Singleton
@Component(modules = {SendEmailModule.class, UserServiceModule.class})
public interface EcommerceProcessorComponent {
  EcommerceProcessorService getEcommerceProcessorService();
}
