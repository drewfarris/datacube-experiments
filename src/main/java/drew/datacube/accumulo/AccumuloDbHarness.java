package drew.datacube.accumulo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.dbharnesses.AfterExecute;
import com.urbanairship.datacube.dbharnesses.FullQueueException;

public class AccumuloDbHarness<T extends Op> implements DbHarness<T> {
	
	public AccumuloDbHarness() {
		
	}
	
	@Override
	public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute)
			throws FullQueueException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<T> get(Address c) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Optional<T>> multiGet(List<Address> addresses)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws InterruptedException {
		// TODO Auto-generated method stub

	}

}
