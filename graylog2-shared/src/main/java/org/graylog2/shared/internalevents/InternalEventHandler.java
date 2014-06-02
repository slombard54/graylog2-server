package org.graylog2.shared.internalevents;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 */
public interface InternalEventHandler {
    public void submit(Object event);
    public void register(Object handler);
    public void unregister(Object handler);
}
